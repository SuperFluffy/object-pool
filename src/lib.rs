use parking_lot::{
    Condvar,
    Mutex,
};
use std::{
    collections::VecDeque,
    mem::{
        ManuallyDrop,
        forget
    },
    ops::{
        Deref,
        DerefMut,
    },
    sync::atomic::{
        AtomicUsize,
        Ordering,
    },
};

pub struct Pool<T> {
    objects: Mutex<VecDeque<T>>,
    object_available: Condvar,
    n_live_objects: AtomicUsize,
    max_capacity: usize,
}

impl<T> Pool<T> {
    // Associates an object with the pool without immediately pushing into it, but returning
    // the object immediately wrapped in a `Reusable` so that it gets pushed into the pool once
    // it goes out of scope.
    pub fn associate(&self, object: T) -> Result<(usize, Reusable<'_, T>), T> {
        let mut n_live_objects = self.n_live_objects.load(Ordering::Relaxed);
        n_live_objects = loop {
            if n_live_objects >= self.max_capacity {
                return Err(object);
            }
            let one_more_object = n_live_objects + 1;
            match self.n_live_objects.compare_exchange_weak(
                n_live_objects,
                one_more_object,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => break one_more_object,
                Err(x) => n_live_objects = x,
            }
        };
        Ok((n_live_objects, Reusable::new(self, object)))
    }

    pub fn associate_with<F>(&self, f: F) -> Option<(usize, Reusable<'_, T>)>
    where
        F: Fn() -> T,
    {
        let mut n_live_objects = self.n_live_objects.load(Ordering::Relaxed);
        n_live_objects = loop {
            if n_live_objects >= self.max_capacity {
                return None;
            }
            let one_more_object = n_live_objects + 1;
            match self.n_live_objects.compare_exchange_weak(
                n_live_objects,
                one_more_object,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => break one_more_object,
                Err(x) => n_live_objects = x,
            }
        };
        let object = f();
        Some((n_live_objects, Reusable::new(self, object)))
    }

    pub fn try_associate_with<E, F>(&self, f: F) -> Result<Option<(usize, Reusable<'_, T>)>, E>
    where
        F: Fn() -> Result<T, E>,
    {
        let mut n_live_objects = self.n_live_objects.load(Ordering::Relaxed);
        n_live_objects = loop {
            if n_live_objects >= self.max_capacity {
                return Ok(None);
            }
            let one_more_object = n_live_objects + 1;
            match self.n_live_objects.compare_exchange_weak(
                n_live_objects,
                one_more_object,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => break one_more_object,
                Err(x) => n_live_objects = x,
            }
        };
        let object = f()?;
        Ok(Some((n_live_objects, Reusable::new(self, object))))
    }

    pub fn capacity(&self) -> usize {
        self.objects.lock().capacity()
    }

    pub fn n_available(&self) -> usize {
        self.objects.lock().len()
    }

    pub fn n_live(&self) -> usize {
        let n_live = self.n_live_objects.load(Ordering::Acquire);
        n_live
    }

    pub fn new(max_capacity: usize) -> Self {
        Self {
            objects: Mutex::new(VecDeque::with_capacity(max_capacity)),
            object_available: Condvar::new(),
            n_live_objects: AtomicUsize::new(0),
            max_capacity, 
        }
    }

    fn attach(&self, object: T) {
        let mut object_lock = self.objects.lock();
        object_lock.push_back(object);
        self.object_available.notify_one();
    }

    /// Push an object into the pool. Returns the number of live objects in the pool in ok position
    /// if the object was successfully pushed into the pool, and otherwise the object in error
    /// position if the pool was at max capacity.
    pub fn push(&self, object: T) -> Result<usize, T> {
        let mut n_live_objects = self.n_live_objects.load(Ordering::Relaxed);
        n_live_objects = loop {
            if n_live_objects >= self.max_capacity {
                return Err(object);
            }
            let one_more_object = n_live_objects + 1;
            match self.n_live_objects.compare_exchange_weak(
                n_live_objects,
                one_more_object,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => break one_more_object,
                Err(x) => n_live_objects = x,
            }
        };
        let mut object_lock = self.objects.lock();
        object_lock.push_back(object);
        self.object_available.notify_one();
        Ok(n_live_objects)
    }

    /// Constructs an object `T` using a closure if the object pool is not at capacity, and pushes
    /// it into the pool. Returns the number of live objects if the object was successfully pushed
    /// into the pool, and `None` otherwise.
    pub fn push_with<F>(&self, f: F) -> Option<usize>
    where
        F: Fn() -> T,
    {
        let mut n_live_objects = self.n_live_objects.load(Ordering::Relaxed);
        n_live_objects = loop {
            if n_live_objects >= self.max_capacity {
                return None;
            }
            let one_more_object = n_live_objects + 1;
            match self.n_live_objects.compare_exchange_weak(
                n_live_objects,
                one_more_object,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => break one_more_object,
                Err(x) => n_live_objects = x,
            }
        };
        let object = f();
        let mut object_lock = self.objects.lock();
        object_lock.push_back(object);
        self.object_available.notify_one();
        Some(n_live_objects)
    }

    /// Constructs an object `T` using a fallible closure if the object pool is not at capacity,
    /// and pushes it into the pool. Returns in ok position the number of live objects if the push
    /// was successful or `None` if the pool was at capacity, and the error `E` from the closure if
    /// constructing the object failed.
    pub fn try_push_with<F, E>(&self, f: F) -> Result<Option<usize>, E>
    where
        F: Fn() -> Result<T, E>,
    {
        let mut n_live_objects = self.n_live_objects.load(Ordering::Relaxed);
        n_live_objects = loop {
            if n_live_objects >= self.max_capacity {
                return Ok(None);
            }
            let one_more_object = n_live_objects + 1;
            match self.n_live_objects.compare_exchange_weak(
                n_live_objects,
                one_more_object,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => break one_more_object,
                Err(x) => n_live_objects = x,
            }
        };
        let object = match f() {
            Ok(object) => object,
            Err(e) => {
                self.n_live_objects.fetch_sub(1, Ordering::Release);
                return Err(e);
            }
        };
        let mut object_lock = self.objects.lock();
        object_lock.push_back(object);
        self.object_available.notify_one();
        Ok(Some(n_live_objects))
    }

    /// Pull an object from the pool. Blocks the current thread until it's notified that another
    /// object is available to be pulled.
    pub fn pull(&self) -> Reusable<'_, T> {
        let mut objects_lock = self.objects.lock();
        while objects_lock.is_empty() {
            self.object_available.wait(&mut objects_lock);
        }
        objects_lock.pop_front().map(|object| Reusable::new(self, object)).unwrap()
    }

    /// Pull an object from the pool. Attempts exactly one pull and returns `None` if the pool
    /// was empty.
    pub fn pull_once(&self) -> Option<Reusable<'_, T>> {
        self.objects.lock().pop_front().map(|object| Reusable::new(self, object))
    }

    /// Pull an object from the pool. Constructs a new object from the supplied closure if no
    /// object is currently available in the pool and if the pool is not yet at capacity.
    pub fn pull_or(&self, object: T) -> Result<Reusable<'_, T>, T> {
        // Attempt to pull an object from the pool; if this failed, construct a new object if
        // the pool is not yet at capacity. Otherwise wait until a new object is available.
        match self.objects.lock().pop_front().map(|object| Reusable::new(self, object)) {
            None => match self.associate(object) {
                Ok((_, reusable_object)) => Ok(reusable_object),
                Err(_) => Ok(self.pull()),
            }
            Some(object) => Ok(object),
        }
    }

    /// Pull an object from the pool. Constructs a new object from the supplied closure if no
    /// object is currently available in the pool and if the pool is not yet at capacity.
    pub fn pull_or_once(&self, object: T) -> Result<Option<Reusable<'_, T>>, T> {
        // Attempt to pull an object from the pool; if this failed, construct a new object if
        // the pool is not yet at capacity. Otherwise wait until a new object is available.
        match self.objects.lock().pop_front().map(|object| Reusable::new(self, object)) {
            None => match self.associate(object) {
                Ok((_, reusable_object)) => Ok(Some(reusable_object)),
                Err(object) => Err(object),
            }
            object @ Some(_) => Ok(object),
        }
    }

    /// Pull an object from the pool. Constructs a new object from the supplied closure if no
    /// object is currently available in the pool and if the pool is not yet at capacity.
    pub fn pull_or_else<F>(&self, f: F) -> Reusable<'_, T>
    where
        F: Fn() -> T,
    {
        // Attempt to pull an object from the pool; if this failed, construct a new object if
        // the pool is not yet at capacity. Otherwise wait until a new object is available.
        match self.objects.lock().pop_front().map(|object| Reusable::new(self, object)) {
            None => match self.associate_with(f) {
                Some((_, reusable_object)) => reusable_object,
                None => self.pull(),
            }
            Some(object) => object,
        }
    }

    pub fn pull_or_else_once<F>(&self, f: F) -> Option<Reusable<'_, T>>
    where
        F: Fn() -> T,
    {
        // Attempt to pull an object from the pool; if this failed, construct a new object if
        // the pool is not yet at capacity. Otherwise wait until a new object is available.
        match self.objects.lock().pop_front().map(|object| Reusable::new(self, object)) {
            None => match self.associate_with(f) {
                Some((_, reusable_object)) => Some(reusable_object),
                None => None,
            }
            object @ Some(_) => object,
        }
    }
}

pub struct Reusable<'a, T> {
    pool: &'a Pool<T>,
    data: ManuallyDrop<T>,
}

impl<'a, T> Reusable<'a, T> {
    fn new(pool: &'a Pool<T>, t: T) -> Self {
        Self {
            pool,
            data: ManuallyDrop::new(t),
        }
    }

    pub fn detach(mut self) -> T {
        let (pool, object) = (self.pool, unsafe { ManuallyDrop::take(&mut self.data) });
        pool.n_live_objects.fetch_sub(1, Ordering::Relaxed);
        forget(self);
        object
    }
}

impl<'a, T> Deref for Reusable<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<'a, T> DerefMut for Reusable<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<'a, T> Drop for Reusable<'a, T> {
    fn drop(&mut self) {
        let inner = unsafe { ManuallyDrop::take(&mut self.data) };
        self.pool.attach(inner);
    }
}

#[cfg(test)]
mod tests {
    use crate::Pool;

    #[test]
    fn detach() {
        let pool: Pool<Vec<usize>> = Pool::new(1);
        assert!(pool.push(Vec::new()).is_ok());
        let mut object = pool.pull().detach();
        object.push(1);
        // Not binding the return value of associate causes the object to be dropped.
        assert!(pool.associate(object).is_ok());
        assert_eq!(pool.pull()[0], 1);
    }

    #[test]
    fn pull() {
        let pool: Pool<Vec<u8>> = Pool::new(8);
        assert!(pool.push(Vec::new()).is_ok());

        let object1 = pool.pull_once();
        let object2 = pool.pull_once();
        let object3 = pool.pull_or_else(|| Vec::new());

        assert!(object1.is_some());
        assert!(object2.is_none());
        drop(object1);
        drop(object2);
        drop(object3);
        assert_eq!(pool.n_available(), 2);
        assert_eq!(pool.n_live(), 2);
    }

    #[test]
    fn e2e() {
        let pool: Pool<Vec<usize>> = Pool::new(8);
        let mut objects = Vec::new();
        for _ in 0..8 {
            assert!(pool.push_with(|| Vec::new()).is_some());
        }
        assert!(pool.push_with(|| Vec::new()).is_none());
        // let mut objects = Vec::new();

        for i in 0..8 {
            let mut object = pool.pull();
            object.push(i);
            objects.push(object);
        }

        assert!(pool.pull_once().is_none());
        assert_eq!(pool.n_available(), 0);
        std::mem::drop(objects);
        assert!(pool.pull_once().is_some());
        assert_eq!(pool.n_available(), pool.n_live());

        for i in 8..0 {
            let mut object = pool.pull_once().unwrap();
            assert_eq!(object.pop(), Some(i));
        }
    }
}
