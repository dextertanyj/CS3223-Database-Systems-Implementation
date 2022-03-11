package simpledb.tx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.BlockId;

/**
 * Manage the transaction's currently-pinned buffers.
 * 
 * @author Edward Sciore
 */
class BufferList {
   private Map<BlockId, Buffer> buffers = new HashMap<>();
   private Map<Integer, Buffer> reserved = new HashMap<>();
   private List<BlockId> pins = new ArrayList<>();
   private BufferMgr bm;

   public BufferList(BufferMgr bm) {
      this.bm = bm;
   }

   /**
    * Return the buffer pinned to the specified block.
    * The method returns null if the transaction has not
    * pinned the block.
    * 
    * @param blk a reference to the disk block
    * @return the buffer pinned to that block
    */
   Buffer getBuffer(BlockId blk) {
      return buffers.get(blk);
   }

   /**
    * Pin the block and keep track of the buffer internally.
    * 
    * @param blk a reference to the disk block
    */
   void pin(BlockId blk) {
      Buffer buff = bm.pin(blk);
      buffers.put(blk, buff);
      pins.add(blk);
   }

   /**
    * Unpin the specified block.
    * 
    * @param blk a reference to the disk block
    */
   void unpin(BlockId blk) {
      Buffer buff = buffers.get(blk);
      bm.unpin(buff);
      pins.remove(blk);
      if (!pins.contains(blk))
         buffers.remove(blk);
   }

   /**
    * Frees the reserved buffer page identified by its ID.
    * 
    * @param int the ID of the reserved buffer page to free.
    */
   public void free(int reservedId) {
      Buffer buff = reserved.get(reservedId);
      bm.unpin(buff);
      reserved.remove(reservedId);
   }

   /**
    * Returns a unique ID representing a reserved buffer page.
    * 
    * @return a unique ID representing a reserved buffer page.
    */
   public int reserve() {
      Buffer buff = bm.reserve();
      int index = reserved.size();
      reserved.put(index, buff);
      return index;
   }

   /**
    * Unpin any buffers still pinned by this transaction.
    */
   void unpinAll() {
      for (BlockId blk : pins) {
         Buffer buff = buffers.get(blk);
         bm.unpin(buff);
      }
      for (Buffer buff : reserved.values()) {
         bm.unpin(buff);
      }
      buffers.clear();
      reserved.clear();
      pins.clear();
   }
}