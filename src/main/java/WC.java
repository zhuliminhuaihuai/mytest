/**
 * WC
 *
 * @author {zhulimin}
 * @date 2020/2/3 0003 下午 14:04
 */
public class WC {

    public String name;
    public Integer one;

    public WC() {
    }

    public WC(String name, Integer one) {
        this.name = name;
        this.one = one;
    }

    @Override
    public String toString() {
        return "WC{" +
                "name='" + name + '\'' +
                ", one=" + one +
                '}';
    }
}
