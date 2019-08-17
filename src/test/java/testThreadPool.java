import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hanjiang2 on 2019/7/30.
 */
public class testThreadPool {
    public static void main(String[] args) {
        // 创建一个可重用固定线程数的线程池
        ExecutorService pool = Executors.newFixedThreadPool(3);;
        // 将线程放入池中进行执行
        for (int i=0; i<9; i++){
            pool.execute(new MyThread(i));
        }
        // 关闭线程池
        pool.shutdown();
    }
}

class MyThread extends Thread {
    int num;
    public MyThread(int i){
        this.num = i;
    }
    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName() + "正在执行。。。" + this.num);
    }
}
