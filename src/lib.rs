//! Multi-producer, multi-consumer FIFO queue communication primitive.
//!
//! This crate provides a multi-producer, multi-consumer, message-based
//! communication channel, concretely defined among two types:
//!
//! * `Sender`
//! * `Receiver`
//!
//! A `Sender` is used to send data to a `Receiver`. Both senders and receivers
//! are clone-able such that sending and receiving can be done concurrently
//! across threads.
//!
//! # Disconnection
//!
//! The send and receive operations will all return a `Result` indicating
//! whether the operation succeeded or not. An unsuccessful operation is
//! normally indicative of the other half of the channel having "hung up" by
//! being dropped in its corresponding thread.
//!
//! Once half of a channel has been deallocated, most operations can no longer
//! continue to make progress, so `Err` will be returned.
//!
//! # Examples
//!
//! Simple usage:
//!
//! ```rust
//! use std::thread;
//!
//! let (tx, rx) = two_lock_queue::channel(1024);
//!
//! for i in 0..10 {
//!     let tx = tx.clone();
//!     thread::spawn(move || {
//!         tx.send(i).unwrap();
//!     });
//! }
//!
//! let mut threads = vec![];
//!
//! for _ in 0..10 {
//!     let rx = rx.clone();
//!     threads.push(thread::spawn(move || {
//!         let j = rx.recv().unwrap();
//!         assert!(0 <= j && j < 10);
//!     }));
//! }
//!
//! for th in threads {
//!     th.join().unwrap();
//! }
//! ```
//!
//! # Algorithm
//!
//! The algorithm is a variant of the Michael-Scott two lock queue found as part
//! of Java's LinkedBlockingQueue. The queue uses a mutex to guard the head
//! pointer and a mutex to guard the tail pointer. Most of the time, send and
//! receive operations will only need to lock a single mutex. An `AtomicUsize`
//! is used to track the number of elements in the queue as well as handle
//! coordination between the producer and consumer halves.

#![deny(warnings, missing_docs)]

pub use std::sync::mpsc::{SendError, TrySendError};
pub use std::sync::mpsc::{TryRecvError, RecvError, RecvTimeoutError};

use std::{mem, ops, ptr, usize};
use std::sync::{Arc, Mutex, MutexGuard, Condvar};
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// The sending-half of the channel.
///
/// Each instance of `Sender` can only be used in a single thread, but it can be
/// cloned to create new `Sender` instances for the same underlying channel and
/// these new  instances can be sent to other threads.
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

/// The receiving-half of the channel.
pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
}

// A variant of the "two lock queue" algorithm.  The `tail` mutex gates entry to
// push elements, and has an associated condition for waiting pushes.  Similarly
// for the `head` mutex.  The "len" field that they both rely on is maintained
// as an atomic to avoid needing to get both locks in most cases. Also, to
// minimize need for pushes to get `head` mutex and vice-versa, cascading
// notifies are used. When a push notices that it has enabled at least one pop,
// it signals `not_empty`. That pop in turn signals others if more items have
// been entered since the signal. And symmetrically for pops signalling pushes.
//
// Visibility between producers and consumers is provided as follows:
//
// Whenever an element is enqueued, the `tail` lock is acquired and `len`
// updated.  A subsequent consumer guarantees visibility to the enqueued Node by
// acquiring the `head` lock and then reading `n = len.load(Acquire)` this gives
// visibility to the first n items.
struct Inner<T> {
    // Maximum number of elements the queue can contain at one time
    capacity: usize,

    // Current number of elements
    len: AtomicUsize,

    // `true` when the channel is currently open
    is_open: AtomicBool,

    // Lock held by take, poll, etc
    head: Mutex<NodePtr<T>>,

    // Wait queue for waiting takes
    not_empty: Condvar,

    // Number of senders in existence
    num_tx: AtomicUsize,

    // Lock held by put, offer, etc
    tail: Mutex<NodePtr<T>>,

    // Wait queue for waiting puts
    not_full: Condvar,

    // Number of receivers in existence
    num_rx: AtomicUsize,
}

/// Possible errors that `send_timeout` could encounter.
pub enum SendTimeoutError<T> {
    /// The channel's receiving half has become disconnected, and there will
    /// never be any more data received on this channel.
    Disconnected(T),
    /// The channel is currently full, and the receiver(s) have not yet
    /// disconnected.
    Timeout(T),
}

/// Creates a new channel of the requested capacity
///
/// Returns the `Sender` and `Receiver` halves.
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let inner = Inner::with_capacity(capacity);
    let inner = Arc::new(inner);

    let tx = Sender { inner: inner.clone() };
    let rx = Receiver { inner: inner };

    (tx, rx)
}

/// Creates a new channel without a capacity bound.
///
/// An alias for `channel(usize::MAX)`
pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    channel(usize::MAX)
}

// ===== impl Sender =====

impl<T> Sender<T> {
    /// Attempts to send a value on this channel, returning it back if it could not be sent.
    ///
    /// A successful send occurs when it is determined that the other end of the
    /// channel has not hung up already. An unsuccessful send would be one where
    /// the corresponding receiver has already been deallocated. Note that a
    /// return value of Err means that the data will never be received, but a
    /// return value of Ok does not mean that the data will be received. It is
    /// possible for the corresponding receiver to hang up immediately after
    /// this function returns Ok.
    ///
    /// This function will **block** the current thread until the channel has
    /// capacity to accept the value or there are no more receivers to accept
    /// the value.
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        self.inner.push(t)
    }

    /// Attempts to send a value on this channel, blocking for at most
    /// `timeout`.
    ///
    /// The function will always block the current thread if the channel has no
    /// available capacity for handling the message. The thread will be blocked
    /// for at most `timeout`, after which, if there still is no capacity,
    /// `SendTimeoutError::Timeout` will be returned.
    pub fn send_timeout(&self, t: T, timeout: Duration) -> Result<(), SendTimeoutError<T>> {
        self.inner.push_timeout(t, timeout)
    }

    /// Attempts to send a value on this channel without blocking.
    ///
    /// This method differs from `send` by returning immediately if the channel's
    /// buffer is full or no receiver is waiting to acquire some data. Compared
    /// with `send`, this function has two failure cases instead of one (one for
    /// disconnection, one for a full buffer).
    ///
    /// See `Sender::send` for notes about guarantees of whether the receiver
    /// has received the data or not if this function is successful.
    pub fn try_send(&self, t: T) -> Result<(), TrySendError<T>> {
        self.inner.try_push(t)
    }

    /// Returns the number of values currently buffered by the channel
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Fully close the channel
    ///
    /// This will force close the channel even if there are outstanding `Sender`
    /// and `Receiver` handles. Further operations on any outstanding handle
    /// will result in a disconnected error.
    pub fn close(&self) {
        self.inner.close();
    }

    /// Returns `true` if the channel is currently in an open state
    pub fn is_open(&self) -> bool {
        self.inner.is_open.load(Ordering::SeqCst)
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Sender<T> {
        // Attempt to clone the inner handle. Doing this before incrementing
        // `num_tx` prevents having to check `num_tx` for overflow and instead
        // rely on `Arc::clone` to prevent the overflow.

        let inner = self.inner.clone();

        // Increment `num_tx`
        self.inner.num_tx.fetch_add(1, Ordering::SeqCst);

        // Return the new sender
        Sender { inner: inner }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if 1 == self.inner.num_tx.fetch_sub(1, Ordering::SeqCst) {
            self.inner.close();
        }
    }
}

// ===== impl Receiver =====

impl<T> Receiver<T> {
    /// Attempts to wait for a value on this receiver, returning an error if the
    /// corresponding channel has hung up.
    ///
    /// This function will always block the current thread if there is no data
    /// available and it's possible for more data to be sent. Once a message is
    /// sent to the corresponding `Sender`, then this receiver will wake up and
    /// return that message.
    ///
    /// If the corresponding Sender has disconnected, or it disconnects while
    /// this call is blocking, this call will wake up and return `Err` to indicate
    /// that no more messages can ever be received on this channel. However,
    /// since channels are buffered, messages sent before the disconnect will
    /// still be properly received.
    pub fn recv(&self) -> Result<T, RecvError> {
        self.inner.pop()
    }

    /// Attempts to wait for a value on this receiver, returning an error if the
    /// corresponding channel has hung up, or if it waits more than timeout.
    ///
    /// This function will always block the current thread if there is no data
    /// available and it's possible for more data to be sent. Once a message is
    /// sent to the corresponding Sender, then this receiver will wake up and
    /// return that message.
    ///
    /// If the corresponding `Sender` has disconnected, or it disconnects while
    /// this call is blocking, this call will wake up and return `Err` to
    /// indicate that no more messages can ever be received on this channel.
    /// However, since channels are buffered, messages sent before the
    /// disconnect will still be properly received.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        self.inner.pop_timeout(timeout)
    }

    /// Attempts to return a pending value on this receiver without blocking
    ///
    /// This method will never block the caller in order to wait for data to
    /// become available. Instead, this will always return immediately with a
    /// possible option of pending data on the channel.
    ///
    /// This is useful for a flavor of "optimistic check" before deciding to
    /// block on a receiver.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.inner.try_pop()
    }

    /// Returns the number of values currently buffered by the channel
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Fully close the channel
    ///
    /// This will force close the channel even if there are outstanding `Sender`
    /// and `Receiver` handles. Further operations on any outstanding handle
    /// will result in a disconnected error.
    pub fn close(&self) {
        self.inner.close();
    }

    /// Returns `true` if the channel is currently in an open state
    pub fn is_open(&self) -> bool {
        self.inner.is_open.load(Ordering::SeqCst)
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Receiver<T> {
        // Attempt to clone the inner handle. Doing this before incrementing
        // `num_rx` prevents having to check `num_rx` for overflow and instead
        // rely on `Arc::clone` to prevent the overflow.

        let inner = self.inner.clone();

        // Increment `num_rx`
        self.inner.num_rx.fetch_add(1, Ordering::SeqCst);

        Receiver { inner: inner }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        if 1 == self.inner.num_rx.fetch_sub(1, Ordering::SeqCst) {
            self.inner.close();
        }
    }
}

impl<T> Inner<T> {
    fn with_capacity(capacity: usize) -> Inner<T> {
        let head = NodePtr::new(Box::new(Node::empty()));

        Inner {
            capacity: capacity,
            len: AtomicUsize::new(0),
            is_open: AtomicBool::new(true),
            head: Mutex::new(head),
            not_empty: Condvar::new(),
            num_tx: AtomicUsize::new(1),
            tail: Mutex::new(head),
            not_full: Condvar::new(),
            num_rx: AtomicUsize::new(1),
        }
    }

    fn len(&self) -> usize {
        self.len.load(Ordering::SeqCst)
    }

    fn push(&self, el: T) -> Result<(), SendError<T>> {
        let node = Box::new(Node::new(el));

        // Acquire the write lock
        let mut tail = self.tail.lock().ok().expect("something went wrong");

        while self.len.load(Ordering::Acquire) == self.capacity {
            // Always check this before sleeping
            if !self.is_open.load(Ordering::Relaxed) {
                return Err(SendError(node.into_inner()));
            }

            tail = self.not_full.wait(tail).ok().expect("something went wrong");
        }

        if !self.is_open.load(Ordering::Relaxed) {
            return Err(SendError(node.into_inner()));
        }

        self.enqueue(node, tail);
        Ok(())
    }

    fn try_push(&self, el: T) -> Result<(), TrySendError<T>> {
        let node = Box::new(Node::new(el));

        // Acquire the write lock
        let tail = self.tail.lock().ok().expect("something went wrong");

        if !self.is_open.load(Ordering::Relaxed) {
            return Err(TrySendError::Disconnected(node.into_inner()));
        }

        if self.len.load(Ordering::Acquire) == self.capacity {
            return Err(TrySendError::Full(node.into_inner()));
        }

        self.enqueue(node, tail);
        Ok(())
    }

    fn push_timeout(&self, el: T, dur: Duration) -> Result<(), SendTimeoutError<T>> {
        let node = Box::new(Node::new(el));

        // Acquire the write lock
        let mut tail = self.tail.lock().ok().expect("something went wrong");

        if self.len.load(Ordering::Acquire) == self.capacity {
            let mut now = Instant::now();
            let deadline = now + dur;

            loop {
                if now >= deadline {
                    return Err(SendTimeoutError::Timeout(node.into_inner()));
                }

                if !self.is_open.load(Ordering::Relaxed) {
                    return Err(SendTimeoutError::Disconnected(node.into_inner()));
                }

                tail = self.not_full
                    .wait_timeout(tail, deadline.duration_since(now))
                    .ok()
                    .expect("something went wrong")
                    .0;

                if self.len.load(Ordering::Acquire) != self.capacity {
                    break;
                }

                now = Instant::now();
            }
        }

        if !self.is_open.load(Ordering::Relaxed) {
            return Err(SendTimeoutError::Disconnected(node.into_inner()));
        }

        self.enqueue(node, tail);

        Ok(())
    }

    fn enqueue(&self, el: Box<Node<T>>, mut tail: MutexGuard<NodePtr<T>>) {
        let ptr = NodePtr::new(el);

        tail.next = ptr;
        *tail = ptr;

        // Increment the count
        let len = self.len.fetch_add(1, Ordering::Release);

        if len + 1 < self.capacity {
            self.not_full.notify_one();
        }

        drop(tail);

        if len == 0 {
            let _l = self.head
                .lock()
                .ok()
                .expect("something went wrong");

            self.not_empty.notify_one();
        }
    }

    fn pop(&self) -> Result<T, RecvError> {
        // Acquire the read lock
        let mut head = self.head.lock().ok().expect("something went wrong");

        while self.len.load(Ordering::Acquire) == 0 {
            // Ensure that there are still senders
            if !self.is_open.load(Ordering::Relaxed) {
                return Err(RecvError);
            }

            head = self.not_empty.wait(head).ok().expect("something went wrong");
        }

        Ok(self.dequeue(head))
    }

    fn try_pop(&self) -> Result<T, TryRecvError> {
        // Acquire the read lock
        let head = self.head.lock().ok().expect("something went wrong");

        if self.len.load(Ordering::Acquire) == 0 {
            if !self.is_open.load(Ordering::Relaxed) {
                return Err(TryRecvError::Disconnected);
            } else {
                return Err(TryRecvError::Empty);
            }
        }

        Ok(self.dequeue(head))
    }

    fn pop_timeout(&self, dur: Duration) -> Result<T, RecvTimeoutError> {
        // Acquire the read lock
        let mut head = self.head.lock().ok().expect("something went wrong");

        if self.len.load(Ordering::Acquire) == 0 {
            let mut now = Instant::now();
            let deadline = now + dur;

            loop {
                if now >= deadline {
                    return Err(RecvTimeoutError::Timeout);
                }

                // Ensure that there are still senders
                if !self.is_open.load(Ordering::Relaxed) {
                    return Err(RecvTimeoutError::Disconnected);
                }

                head = self.not_empty
                    .wait_timeout(head, deadline.duration_since(now))
                    .ok()
                    .expect("something went wrong")
                    .0;

                if self.len.load(Ordering::Acquire) != 0 {
                    break;
                }

                now = Instant::now();
            }
        }

        // At this point, we are guaranteed to be able to dequeue a value
        Ok(self.dequeue(head))
    }

    fn dequeue(&self, mut head: MutexGuard<NodePtr<T>>) -> T {
        let h = *head;
        let mut first = h.next;

        *head = first;

        let val = first.item.take().expect("item already consumed");
        let len = self.len.fetch_sub(1, Ordering::Release);

        if len > 1 {
            self.not_empty.notify_one();
        }

        // Release the lock here so that acquire the write lock does not result
        // in a deadlock
        drop(head);

        // Free memory
        h.free();

        if len == self.capacity {
            let _l = self.tail
                .lock()
                .ok()
                .expect("something went wrong");

            self.not_full.notify_one();
        }

        val
    }

    fn close(&self) {
        if self.is_open.swap(false, Ordering::SeqCst) {
            self.notify_tx();
            self.notify_rx();
        }
    }

    fn notify_tx(&self) {
        let _lock = self.head.lock().expect("something went wrong");
        self.not_empty.notify_all();
    }

    fn notify_rx(&self) {
        let _lock = self.tail.lock().expect("something went wrong");
        self.not_full.notify_all();
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        while let Ok(_) = self.try_pop() {
        }
    }
}

struct Node<T> {
    next: NodePtr<T>,
    item: Option<T>,
}

impl<T> Node<T> {
    fn new(val: T) -> Node<T> {
        Node {
            next: NodePtr::null(),
            item: Some(val),
        }
    }

    fn empty() -> Node<T> {
        Node {
            next: NodePtr::null(),
            item: None,
        }
    }

    fn into_inner(self) -> T {
        self.item.unwrap()
    }
}

struct NodePtr<T> {
    ptr: *mut Node<T>,
}

impl<T> NodePtr<T> {
    fn new(node: Box<Node<T>>) -> NodePtr<T> {
        NodePtr { ptr: unsafe { mem::transmute(node) } }
    }

    fn null() -> NodePtr<T> {
        NodePtr { ptr: ptr::null_mut() }
    }

    fn free(self) {
        let NodePtr { ptr } = self;
        let _: Box<Node<T>> = unsafe { mem::transmute(ptr) };
    }
}

impl<T> ops::Deref for NodePtr<T> {
    type Target = Node<T>;

    fn deref(&self) -> &Node<T> {
        unsafe { mem::transmute(self.ptr) }
    }
}

impl<T> ops::DerefMut for NodePtr<T> {
    fn deref_mut(&mut self) -> &mut Node<T> {
        unsafe { mem::transmute(self.ptr) }
    }
}

impl<T> Clone for NodePtr<T> {
    fn clone(&self) -> NodePtr<T> {
        NodePtr { ptr: self.ptr }
    }
}

impl<T> Copy for NodePtr<T> {}
unsafe impl<T> Send for NodePtr<T> {}

#[cfg(test)]
mod test {
    use super::*;
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn single_thread_send_recv() {
        let (tx, rx) = channel(1024);

        // Check the length
        assert_eq!(0, tx.len());
        assert_eq!(0, rx.len());

        // Send a value
        tx.send("hello").unwrap();

        // Check the length again
        assert_eq!(1, tx.len());
        assert_eq!(1, rx.len());

        // Receive the value
        assert_eq!("hello", rx.recv().unwrap());

        // Check the length
        assert_eq!(0, tx.len());
        assert_eq!(0, rx.len());

        // Try taking on an empty queue
        assert_eq!(TryRecvError::Empty, rx.try_recv().unwrap_err());
    }

    #[test]
    fn single_thread_send_timeout() {
        let (tx, _rx) = channel(1);

        tx.try_send("hello").unwrap();

        let now = Instant::now();
        let dur = Duration::from_millis(200);

        assert!(tx.send_timeout("world", dur).is_err());

        let act = now.elapsed();

        assert!(act >= dur);
        assert!(act < dur * 2);
    }

    #[test]
    fn single_thread_recv_timeout() {
        let (_tx, rx) = channel::<u32>(1024);

        let now = Instant::now();
        let dur = Duration::from_millis(200);

        assert!(rx.recv_timeout(dur).is_err());

        let act = now.elapsed();

        assert!(act >= dur);
        assert!(act < dur * 2);
    }

    #[test]
    fn single_consumer_single_producer() {
        let (tx, rx) = channel(1024);

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));

            for i in 0..10_000 {
                tx.send(i).unwrap();
            }
        });

        for i in 0..10_000 {
            assert_eq!(i, rx.recv().unwrap());
        }

        assert!(rx.recv().is_err());
    }

    #[test]
    fn single_consumer_multi_producer() {
        let (tx, rx) = channel(1024);

        for t in 0..10 {
            let tx = tx.clone();

            thread::spawn(move || {
                for i in 0..10_000 {
                    tx.send((t, i)).unwrap();
                }
            });
        }

        drop(tx);

        let mut vals = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

        for _ in 0..10 * 10_000 {
            let (t, v) = rx.recv().unwrap();
            assert_eq!(vals[t], v);
            vals[t] += 1;
        }

        assert!(rx.recv().is_err());

        for i in 0..10 {
            assert_eq!(vals[i], 10_000);
        }
    }

    #[test]
    fn multi_consumer_multi_producer() {
        let (tx, rx) = channel(1024);
        let (res_tx, res_rx) = channel(1024);

        const PER_PRODUCER: usize = 10_000;

        // Producers
        for t in 0..5 {
            let tx = tx.clone();

            thread::spawn(move || {
                for i in 1..PER_PRODUCER {
                    tx.send((t, i)).unwrap();

                    if i % 10 == 0 {
                        thread::yield_now();
                    }
                }
            });
        }

        drop(tx);

        // Consumers
        for _ in 0..5 {
            let rx = rx.clone();
            let res_tx = res_tx.clone();

            thread::spawn(move || {
                let mut vals = vec![];
                let mut per_producer = [0, 0, 0, 0, 0];

                loop {
                    let (t, v) = match rx.recv() {
                        Ok(v) => v,
                        _ => break,
                    };

                    assert!(per_producer[t] < v);
                    per_producer[t] = v;

                    vals.push((t, v));

                    if v % 10 == 0 {
                        thread::yield_now();
                    }
                }

                res_tx.send(vals).unwrap();
            });
        }

        drop(rx);
        drop(res_tx);

        let mut all_vals = vec![];

        for _ in 0..5 {
            let vals = res_rx.recv().unwrap();

            for &v in vals.iter() {
                all_vals.push(v);
            }
        }

        all_vals.sort();

        let mut per_producer = [1, 1, 1, 1, 1];

        for &(t, v) in all_vals.iter() {
            assert_eq!(per_producer[t], v);
            per_producer[t] += 1;
        }

        for &v in per_producer.iter() {
            assert_eq!(PER_PRODUCER, v);
        }
    }

    #[test]
    fn queue_with_capacity() {
        let (tx, rx) = channel(8);

        for i in 0..8 {
            assert!(tx.try_send(i).is_ok());
        }

        assert_eq!(TrySendError::Full(8), tx.try_send(8).unwrap_err());
        assert_eq!(0, rx.try_recv().unwrap());

        assert!(tx.try_send(8).is_ok());

        for i in 1..9 {
            assert_eq!(i, rx.try_recv().unwrap());
        }
    }

    #[test]
    fn multi_producer_at_capacity() {
        let (tx, rx) = channel(8);

        for _ in 0..8 {
            let tx = tx.clone();

            thread::spawn(move || {
                for i in 0..1_000 {
                    tx.send(i).unwrap();
                }
            });
        }

        drop(tx);

        for _ in 0..8 * 1_000 {
            rx.recv().unwrap();
        }

        rx.recv().unwrap_err();
    }

    #[test]
    fn test_tx_shutdown() {
        let (tx, rx) = channel(1024);

        {
            // Clone tx to keep a handle open
            let tx = tx.clone();
            thread::spawn(move || {
                tx.send("hello").unwrap();
                tx.close();
            });
        }

        assert_eq!("hello", rx.recv().unwrap());
        assert!(rx.recv().is_err());
        assert!(tx.send("goodbye").is_err());
    }

    #[test]
    fn test_rx_shutdown() {
        let (tx, rx) = channel(1024);

        {
            let tx = tx.clone();
            let rx = rx.clone();

            thread::spawn(move || {
                tx.send("hello").unwrap();
                rx.close();
            });
        }

        assert_eq!("hello", rx.recv().unwrap());
        assert!(rx.recv().is_err());
        assert!(tx.send("goodbye").is_err());
    }
}
