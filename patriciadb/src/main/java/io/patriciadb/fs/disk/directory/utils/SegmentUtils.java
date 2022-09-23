package io.patriciadb.fs.disk.directory.utils;


import java.util.ArrayList;
import java.util.List;

public class SegmentUtils {

    public static List<FileSegment> calculateSegments(long fileSize, long maxSegmentSize) {
        long cursor = 0;
        var result = new ArrayList<FileSegment>();
        while(fileSize>0) {
            long segmentSize = Math.min(fileSize, maxSegmentSize);
            result.add(new FileSegment(cursor,segmentSize));
            cursor+=segmentSize;
            fileSize-=segmentSize;
        }
        return result;
    }

    public record FileSegment(long initialPosition, long length) {

    }
}
