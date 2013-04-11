package ref.util;


/**
 * 循环数组
 *
 * @param <T>
 */
public class Circular<T> {
    private T[] elements;
    private int cursor; //游标

    public Circular(int size) {
        this.elements = (T[]) new Object[size];
    }

    public T add(T element) {

        if (cursor == elements.length - 1) {
            cursor = -1;
        }
        cursor++;
        T oldElement = elements[cursor];
        elements[cursor] = element;
        return oldElement;
    }

    public T get(int i) {
        i = cursor - i;
        if (i < 0) {
            i += elements.length;
        }
        return elements[i];
    }

    public int size() {
        return elements.length;
    }
}
