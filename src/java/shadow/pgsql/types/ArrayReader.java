package shadow.pgsql.types;

/**
 * Created by zilence on 09.08.14.
 */
public interface ArrayReader<G, A, T> {
    G init(int size);

    G add(G arr, int index, T item);

    G addNull(G arr, int index);

    A complete(G arr);
}
