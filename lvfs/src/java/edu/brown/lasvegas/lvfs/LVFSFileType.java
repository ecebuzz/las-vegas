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

    /** Sparse position file. */
    POSITION_FILE {
        @Override
        public String getExtension() {
            return "pos";
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
    
    public static LVFSFileType getFromExtension (String extension) {
        if (extension.equals(DATA_FILE.getExtension())) return DATA_FILE;
        if (extension.equals(POSITION_FILE.getExtension())) return POSITION_FILE;
        if (extension.equals(DICTIONARY_FILE.getExtension())) return DICTIONARY_FILE;
        return null;
    }
}
