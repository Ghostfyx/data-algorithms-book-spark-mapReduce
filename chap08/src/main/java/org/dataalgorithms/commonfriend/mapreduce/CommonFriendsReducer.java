package org.dataalgorithms.commonfriend.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * @description:
 * @author: fanyeuxiang
 * @createDate: 2020-01-19
 */
public class CommonFriendsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> map = new HashMap<String, Integer>();
        Iterator<Text> iterator = values.iterator();
        int numOfValues = 0;
        while (iterator.hasNext()) {
            String friends = iterator.next().toString();
            if (friends.equals("")) {
                context.write(key, new Text("[]"));
                return;
            }
            addFriends(map, friends);
            numOfValues++;
        }

        // now iterate the map to see how many have numOfValues
        List<String> commonFriends = new ArrayList<String>();
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            //System.out.println(entry.getKey() + "/" + entry.getValue());
            if (entry.getValue() == numOfValues) {
                commonFriends.add(entry.getKey());
            }
        }

        // sen it to output
        context.write(key, new Text(commonFriends.toString()));
    }

    public void addFriends(Map<String, Integer> map, String friendsList) {
        String[] friends = StringUtils.split(friendsList, ",");
        for (String friend : friends) {
            Integer count = map.get(friend);
            if (count == null) {
                map.put(friend, 1);
            } else {
                map.put(friend, ++count);
            }
        }
    }

}
