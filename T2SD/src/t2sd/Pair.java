package t2sd;


import java.io.Serializable;

/**
 *
 * @author Carol pc
 * Utility class that stores a pair of string and integer that is serializable
 */

class Pair<X extends Serializable, Y extends Serializable> implements Serializable {

    private X mID;

    //Quantity of acks received to the message with the mId = this.mID
    private Y qnt;

    public Pair(X mID, Y qnt) {
        this.mID = mID;
        this.qnt = qnt;
    }

    public X getmID() {
        return mID;
    }

    public Y getQnt() {
        return qnt;
    }

    public void setQnt(Y qnt) {
        this.qnt = qnt;
    }

}
