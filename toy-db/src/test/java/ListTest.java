import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ListTest {
    @Test
    public void test(){
        List<Integer> list = new ArrayList<>();

        list.add(0);
        list.add(1);
        list.add(2);
        list.add(3);

        System.out.println(list.size());

        list.remove(3);
        list.remove(2);

        System.out.println(list.size());
    }
}
