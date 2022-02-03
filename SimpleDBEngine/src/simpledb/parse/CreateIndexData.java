package simpledb.parse;

/**
 * The parser for the <i>create index</i> statement.
 * 
 * @author Edward Sciore
 */
public class CreateIndexData {
   public enum IndexType {
      HASH("hash"), BTREE("btree");

      private final String val;

      private IndexType(String val) {
         this.val = val;
      }

      public String getVal() {
         return this.val;
      }

      public static IndexType createIndexType(String idxTypeStr) {
         if (HASH.getVal().equals(idxTypeStr)) {
            return HASH;
         } else if (BTREE.getVal().equals(idxTypeStr)) {
            return BTREE;
         } else {
            assert false: "unrecognised string";
            return BTREE;
         }
      }
   }

   private String idxname, tblname, fldname;
   private IndexType idxType;

   /**
    * Saves the table and field names of the specified index.
    */
   public CreateIndexData(String idxname, String tblname, String fldname) {
      this.idxname = idxname;
      this.tblname = tblname;
      this.fldname = fldname;
      this.idxType = IndexType.HASH;
   }

   public CreateIndexData(String idxname, String tblname, String fldname, IndexType idxtype) {
      this.idxname = idxname;
      this.tblname = tblname;
      this.fldname = fldname;
      this.idxType = idxtype;
   }

   /**
    * Returns the name of the index.
    * 
    * @return the name of the index
    */
   public String indexName() {
      return idxname;
   }

   /**
    * Returns the name of the indexed table.
    * 
    * @return the name of the indexed table
    */
   public String tableName() {
      return tblname;
   }

   /**
    * Returns the name of the indexed field.
    * 
    * @return the name of the indexed field
    */
   public String fieldName() {
      return fldname;
   }

   public IndexType idxType() {
      return idxType;
   }
}
