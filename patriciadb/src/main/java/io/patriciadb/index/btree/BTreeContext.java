package io.patriciadb.index.btree;

public class BTreeContext {

    private final static int M=21;
    private final static int M_HALF = M/2;

    public boolean isUnique() {
        return true;
    }

    public int getM() {
        return M;
    }

    public int getM_Half(){
        return M_HALF;
    }
}
