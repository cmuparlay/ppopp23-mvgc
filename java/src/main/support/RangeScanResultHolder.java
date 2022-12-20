package main.support;

/**
 Represents a storage space where the result of a range query operation is saved
 Each thread gets a copy of this variable
 */
public class RangeScanResultHolder {
    public Stack rsResult;

    public RangeScanResultHolder() {
        rsResult = new Stack();
    }

    public static final class Stack {
        private final int INIT_SIZE = 16;
        private Object[] stackArray;
        private int head = 0;

        public Stack() {
            stackArray = new Object[INIT_SIZE];
        }

        public void clear() {
            head = 0;
        }

        public Object[] getStackArray() {
            return stackArray;
        }

        public int getEffectiveSize() {
            return head;
        }

        public void push(final Object x) {
            if (head == stackArray.length) {
                final Object[] newStackArray = new Object[stackArray.length*4];
                System.arraycopy(stackArray, 0, newStackArray, 0, head);
                stackArray = newStackArray;
            }
            stackArray[head] = x;
            ++head;
        }
    }
}