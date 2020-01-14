package org.algorithms.orderinversion.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @description: 词频反转统计的组合键<K1, K2>
 * WritableComparable representing a pair of Strings.
 * The elements in the pair are referred to as the left (word)
 * and right (neighbor) elements. The natural sort order is:
 * first by the left element, and then by the right element.
 *
 * @author: fanyeuxiang
 * @createDate: 2020-01-14
 */
public class PairOfWords implements WritableComparable<PairOfWords> {

    /**
     * current word
     */
    private String leftWord;

    /**
     * neighbor word
     */
    private String rightWord;

    public PairOfWords() {
    }

    public PairOfWords(String leftWord, String rightWord) {
        this.leftWord = leftWord;
        this.rightWord = rightWord;
    }

    public String getLeftWord() {
        return leftWord;
    }

    public void setLeftWord(String leftWord) {
        this.leftWord = leftWord;
    }

    public String getRightWord() {
        return rightWord;
    }

    public void setRightWord(String rightWord) {
        this.rightWord = rightWord;
    }

    public void setWord(String leftElement) {
        this.leftWord = leftElement;
    }

    public void setNeighbor(String rightElement) {
        this.rightWord = rightElement;
    }

    @Override
    public int compareTo(PairOfWords otheWords) {
        String otherLeft = otheWords.getLeftWord();
        String otherRight = otheWords.getRightWord();
        if (this.leftWord.equals(otherLeft)){
            return this.rightWord.compareTo(otherRight);
        }
        return this.leftWord.compareTo(otherLeft);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Text.writeString(dataOutput, leftWord);
        Text.writeString(dataOutput, rightWord);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.leftWord = Text.readString(dataInput);
        this.rightWord = Text.readString(dataInput);
    }

    /**
     * Generates human-readable String representation of this pair.
     *
     * @return human-readable String representation of this pair
     */
    @Override
    public String toString() {
        return "(" + leftWord + ", " + rightWord + ")";
    }
}
