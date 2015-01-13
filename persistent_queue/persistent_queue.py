from cffi import FFI
import pickle
import os

# Get the directory the script is currently running in, so that we can access all the shared objects
# correctly.
# TODO: Look at other python projects with C bindings and see if there's a better way to do this
script_directory = os.path.dirname(os.path.abspath(__file__))

_PERSISTENT_QUEUE_DECLARATIONS = ""

# TODO: Clean this up.  This is kind of a hack to avoid copying that header over here.  We also need
# to remove lines starting with "#", since the FFI library doesn't like anything that's meant for
# the preprocessor.  Maybe we can just run the preprocessor here?
with open(os.path.join(script_directory, "storage_manager.h")) as f:
    for line in f:
        if line[0] != "#":
            _PERSISTENT_QUEUE_DECLARATIONS += line

# Initialize C bindings
ffi = FFI()
ffi.cdef(_PERSISTENT_QUEUE_DECLARATIONS)
ffi.dlopen(None)
# TODO: Actually just set rpath on libsoftheap.so correctly, since it depends on libck
ffi.dlopen(os.path.join(script_directory, "./libck.so.0.4.3"))
sm_lib = ffi.dlopen(os.path.join(script_directory,"./libsoftheap.so"))

class PersistentQueue(object):
    """Persistent String Queue Implemented in C"""

    def __init__(self):

        # Whether this queue actually has an active storage manager
        self.active = False

    def open(self, queue_dir):
        """Open the data files of this persistent queue"""
        assert self.active is False

        # Save the queue_dir so we can remove it if we need to destroy the queue
        self.queue_dir = queue_dir

        # Save these so we are sure that they last longer than the underlying queue, since the
        # underlying queue takes these as "const char*" and doesn't make copies of them.
        self.c_queue_dir = ffi.new("const char[]", self.queue_dir)
        self.c_queue_name = ffi.new("const char[]", "queuedata")

        # Reopen the queue if it exists.  This is to be consistent with the way queuelib's
        # persistent disk queue works.
        # TODO: There is a bad case here if we do something like this:
        # q = PersistentQueue("samedir")
        # q = PersistentQueue("samedir")
        # Because the constructor runs first, so "q" will be a queue with a data directory that
        # doesn't exist.  Should do better checking of this, but how?  Can we lock a directory?
        if os.path.exists(queue_dir):
            self.sm = sm_lib.open_storage_manager(self.c_queue_dir, self.c_queue_name, 32 * 1024 * 1024, 1)
        else:
            os.makedirs(queue_dir)
            self.sm = sm_lib.create_storage_manager(self.c_queue_dir, self.c_queue_name, 32 * 1024 * 1024, 1)

        # Now this queue actually has an active storage manager
        self.active = True

    def push(self, item):
        """Push a string item onto the persistent queue"""
        assert self.active is True

        # Serialize this item and allocate a new C byte buffer for writing into the queue
        c_item = ffi.new("char[]", pickle.dumps(item))

        # TODO: Handle write errors
        ret = self.sm.write(self.sm, c_item, len(c_item))
        return ret

    def pop(self):
        """Pop an item off of the persistent queue.  This item must be explicitly freed"""
        assert self.active is True
        cursor = self.sm.pop_cursor(self.sm)

        # We may have data in the queue that has not yet been synced.  Sync the data that we have in
        # the queue and try again.  If we still don't get anything, then the queue was empty at the
        # time we called sync.
        if cursor == ffi.NULL:
            self.sm.sync(self.sm, 1)
            cursor = self.sm.pop_cursor(self.sm)
            if cursor == ffi.NULL:
                return None

        # Convert the value from a C void* to a python string
        raw_value = ffi.string(ffi.cast("char*", cursor.data), cursor.size)

        # Convert the value back into its original object
        # TODO: This could be slow, but I think the data has to be serialized for it to be storable
        # in the queue.
        value = pickle.loads(raw_value)

        # Free the underlying cursor, since we've already extracted the data we need
        self.sm.free_cursor(self.sm, cursor)

        return value

    # Queue destruction methods
    def __del__(self):

        # Make sure this queue was explicity destroyed
        # XXX: This is for debugging, but is this really any way to behave?  Maybe I should just
        # call close when in doubt.
        assert self.active is False

    def destroy(self):
        """Destroy this queue.  Deletes underlying files"""
        assert self.active is True
        self.active = False
        self.sm.destroy(self.sm)
        for f in os.listdir(self.queue_dir):
            os.remove(f)
        os.rmdir(self.queue_dir)

    def close(self):
        """Close this queue.  Does not delete underlying files"""
        assert self.active is True
        self.active = False
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

    q = PersistentQueue()
    q.open("queue_test")
    q.push({"a":1})
    item = q.pop()
    print(item)

    for i in range(1000):
        print("Inserting item number " + str(i))
        q.push("Item number " + str(i))

    for item in q:
        print(item)

    q.destroy()

    q2 = PersistentQueue()
    q2.open("queue2_test")
    for i in range(1000):
        print("Inserting item number " + str(i))
        q2.push("Item number " + str(i))

    for item in q2:
        print(item)

    q2.destroy()
