from cffi import FFI
import pickle
import os

_PERSISTENT_QUEUE_DECLARATIONS = ""

# TODO: Clean this up.  This is kind of a hack to avoid copying that header over here.  We also need
# to remove lines starting with "#", since the FFI library doesn't like anything that's meant for
# the preprocessor.  Maybe we can just run the preprocessor here?
with open("include/storage_manager.h") as f:
    for line in f:
        if line[0] != "#":
            _PERSISTENT_QUEUE_DECLARATIONS += line

# Initialize C bindings
ffi = FFI()
ffi.cdef(_PERSISTENT_QUEUE_DECLARATIONS)
ffi.dlopen(None)
# TODO: Actually just set rpath on libsoftheap.so correctly, since it depends on libck
ffi.dlopen("./ck/lib/libck.so.0")
sm_lib = ffi.dlopen("./libsoftheap.so")

class PersistentQueue(object):
    """Persistent String Queue Implemented in C"""

    def __init__(self, queue_dir):

        # We need to keep this around otherwise it will get garbage collected
        # TODO: Understand better what was going on here.  I think this is sufficient, but I'm not
        # sure.  The interpreter could be doing other optimizations that I'm not aware of.
        # The issue is that the "open_storage_manager" function takes a const char*, so it directly
        # uses the value that gets passed in, rather than making a copy.
        self.name = str(queue_dir + "/queuedata")

        # Save the queue_dir so we can remove it if we need to destroy the queue
        self.queue_dir = queue_dir

        # Reopen the queue if it exists.  This is to be consistent with the way queuelib's
        # persistent disk queue works.
        # TODO: There is a bad case here if we do something like this:
        # q = PersistentQueue("samedir")
        # q = PersistentQueue("samedir")
        # Because the constructor runs first, so "q" will be a queue with a data directory that
        # doesn't exist.  Should do better checking of this, but how?  Can we lock a directory?
        if os.path.exists(queue_dir):
            self.sm = sm_lib.open_storage_manager(".", self.name, 1024 * 1024, 1)
        else:
            os.makedirs(queue_dir)
            self.sm = sm_lib.create_storage_manager(".", self.name, 1024 * 1024, 1)
        self.length = 0
        self.destroyed = False

    def push(self, item):
        """Push a string item onto the persistent queue"""
        assert self.destroyed is False

        # Serialize this item and allocate a new C byte buffer for writing into the queue
        c_item = ffi.new("char[]", pickle.dumps(item))

        # TODO: Handle write errors
        ret = self.sm.write(self.sm, c_item, len(c_item))

        # Keep track of the length of this queue
        # TODO: Make this persistent
        self.length = self.length + 1
        return ret

    def pop(self):
        """Pop an item off of the persistent queue.  This item must be explicitly freed"""
        assert self.destroyed is False
        cursor = self.sm.pop_cursor(self.sm)

        # We may have data in the queue that has not yet been synced.  Sync the data that we have in
        # the queue and try again.  If we still don't get anything, then the queue was empty at the
        # time we called sync.
        if (cursor == ffi.NULL):
            self.sm.sync(self.sm, 1)
            cursor = self.sm.pop_cursor(self.sm)
            if (cursor == ffi.NULL):
                return None

        # Convert the value from a C void* to a python string
        raw_value = ffi.string(ffi.cast("char*", cursor.data), cursor.size)

        # Convert the value back into its original object
        # TODO: This could be slow, but I think the data has to be serialized for it to be storable
        # in the queue.
        value = pickle.loads(raw_value)

        # Free the underlying cursor, since we've already extracted the data we need
        self.sm.free_cursor(self.sm, cursor)

        # Keep track of the length of this queue
        # TODO: This is not persistent
        self.length = self.length - 1

        return value

    def __len__(self):
        return self.length

    # Queue destruction methods
    def __del__(self):

        # Only destroy this queue if it was not explicitly closed using one of the functions below
        if self.destroyed is False:
            self.destroy()

    def destroy(self):
        """Destroy this queue.  Deletes underlying files"""
        assert self.destroyed is False
        self.destroyed = True
        self.sm.destroy(self.sm)

        # Because the order of destruction is not explicit, the "os" module may be unloaded before
        # this destructor gets called, so reload it here.
        import os
        os.rmdir(self.queue_dir)

    # NOTE:  Queuelib's priority queue calls this function.  That might mean that it has the ability
    # to reopen existing queues.  However, this happens implicitly if the data file exists already,
    # rather than explicitly the way we do by passing an argument to the constructor.  Make our
    # behaviour match queuelib for now.
    def close(self):
        """Close this queue.  Does not delete underlying files"""
        assert self.destroyed is False
        self.destroyed = True
        self.sm.close(self.sm)

    # Make this queue an iterator
    def __iter__(self):
        return self

    def __next__(self):
        return self.pop()

    def next(self):
        item = self.pop()
        if (item is None):
            raise StopIteration
        else:
            return item

if __name__ == '__main__':

    q = PersistentQueue("queue_test")
    q.push({"a":1})
    print(len(q))
    item = q.pop()
    print(item)

    for i in range(1000):
        print("Inserting item number " + str(i))
        q.push("Item number " + str(i))

    print(len(q))

    for item in q:
        print(item)

    q2 = PersistentQueue("queue2_test")
    for i in range(1000):
        print("Inserting item number " + str(i))
        q2.push("Item number " + str(i))

    print(len(q2))

    for item in q2:
        print(item)
