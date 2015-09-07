package t2sd;


import java.io.Serializable;

/**
 * Utility class that stores a pair of string and integer that is serializable
 */

class Pair<X extends Serializable, Y extends Serializable> implements Serializable {

    private X message;

    //Quantity of acks received to the message with the mId = this.message
    private Y mID;

    public Pair(X message, Y mID) {
        this.message = message;
        this.mID = mID;
    }

    public X getMessage() {
        return message;
    }

    public Y getmID() {
        return mID;
    }

    public void setmID(Y mID) {
        this.mID = mID;
    }

}
