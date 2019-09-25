/*
 * Copyright 2015 The FireNio Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging;

/**
 * @author wangkai
 */
public class QuickSort {

    interface SortValue {
        long get(Message message);
    }

    static final SortValue SORT_VALUE_A = message -> message.getA();
    static final SortValue SORT_VALUE_T = message -> message.getT();

    private static int partition(Message[] array, SortValue sort_value, int low, int high) {
        //三数取中
        int mid = low + (high - low) / 2;
        if (sort_value.get(array[mid]) > sort_value.get(array[high])) {
            swap(array, mid, high);
        }
        if (sort_value.get(array[low]) > sort_value.get(array[high])) {
            swap(array, low, high);
        }
        if (sort_value.get(array[mid]) > sort_value.get(array[low])) {
            swap(array, mid, low);
        }
        Message key   = array[low];
        long    key_a = sort_value.get(key);

        while (low < high) {
            while (sort_value.get(array[high]) >= key_a && high > low) {
                high--;
            }
            array[low] = array[high];
            while (sort_value.get(array[low]) <= key_a && high > low) {
                low++;
            }
            array[high] = array[low];
        }
        array[low] = key;
        return high;
    }

    public static void sort(Message[] array, SortValue sort_value, int low, int high) {
        if (low >= high) {
            return;
        }
        int index = partition(array, sort_value, low, high);
        sort(array, sort_value, low, index - 1);
        sort(array, sort_value, index + 1, high);
    }

    private static void swap(Message[] array, int a, int b) {
        Message temp = array[a];
        array[a] = array[b];
        array[b] = temp;
    }

}
