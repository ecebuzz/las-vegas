package edu.brown.lasvegas.lvfs;

/**
 * Represents a type of a file in LVFS.
 */
public enum LVFSFileType {
    /** Main data file (which might be compressed).*/
    DATA_FILE {
        @Override
        public String getExtension() {
            return "dat";
        }
    },

    /** Temporary data file which is only tentatively used while dictionary compression.*/
    TMP_DATA_FILE {
        @Override
        public String getExtension() {
            return "tmp";
        }
    },

    /** Sparse position index file. */
    POSITION_FILE {
        @Override
        public String getExtension() {
            return "pos";
        }
    },

    /** Sparse value index file for sorting columns. */
    VALUE_FILE {
        @Override
        public String getExtension() {
            return "vdx";
        }
    },

    /** Dictionary file for dictionary compression. */
    DICTIONARY_FILE {
        @Override
        public String getExtension() {
            return "dic";
        }
    },
    ;
    
    /** Gives the file extension for the file type. */
    public abstract String getExtension();
    
    public String appendExtension (String filename) {
        return filename + "." + getExtension();
    }
    
    public static LVFSFileType getFromExtension (String extension) {
        if (extension.equals(DATA_FILE.getExtension())) return DATA_FILE;
        if (extension.equals(TMP_DATA_FILE.getExtension())) return TMP_DATA_FILE;
        if (extension.equals(POSITION_FILE.getExtension())) return POSITION_FILE;
        if (extension.equals(VALUE_FILE.getExtension())) return VALUE_FILE;
        if (extension.equals(DICTIONARY_FILE.getExtension())) return DICTIONARY_FILE;
        return null;
    }
}
