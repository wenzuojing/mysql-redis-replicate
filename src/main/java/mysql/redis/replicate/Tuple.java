package mysql.redis.replicate;

/**
 * Created by wens on 15-10-22.
 */
public class Tuple<T1, T2> {

    private T1 one;
    private T2 two;

    public Tuple(T1 one, T2 two) {
        this.one = one;
        this.two = two;
    }

    public T1 getOne() {
        return one;
    }

    public void setOne(T1 one) {
        this.one = one;
    }

    public T2 getTwo() {
        return two;
    }

    public void setTwo(T2 two) {
        this.two = two;
    }

    public static <T1, T2> Tuple<T1, T2> of(T1 v1, T2 v2) {
        return new Tuple<>(v1, v2);
    }

}
