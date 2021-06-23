package org.act.temporalProperty.impl;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import org.act.temporalProperty.query.aggr.AggregationQuery;

import java.io.File;
import java.io.IOException;
import java.util.List;
/**
 * 将文件编号转化为文件名的工具类
 *
 */
public final class Filename
{
    private Filename()
    {
    }

    /**
     * 各种文件的类型
     */
    public enum FileType
    {
        STBUFFER,
        BUFFER,
        LOG,
        DB_LOCK,
        STABLEFILE,
        UNSTABLEFILE,
        DESCRIPTOR,
        CURRENT,
        TEMP,
        INFO_LOG  // Either the current one, or an old one
    }

    public static String stbufferFileName( long number)
    {
        return makeFileName( number, "st", "buffer" );
    }
    
    public static String unbufferFileName(long number)
    {
        return makeFileName( number, "un", "buffer" );
    }
    
    /**
     * 返回对应编号的日志文件的名称
     */
    public static String logFileName(long number)
    {
        return makeFileName(number, "log");
    }

    /**
     * 返回对应编号的StableFile文件的名称.
     */
    public static String stableFileName(long number)
    {
        return makeFileName(number, "st", "table");
    }

    public static String stableFileName(int propertyId, long number)
    {
        return propertyId+"/"+makeFileName(number, "st", "table");
    }

    public static String stPath(File proDir, long fileNumber) {
        AggregationQuery.cnt[0]++;
        return new File(proDir, stableFileName(fileNumber)).getAbsolutePath();
    }
    
    /**
     * 返回对应编号的UnStableFile文件的名称
     */
    public static String unStableFileName(long number)
    {
        return makeFileName( number, "un", "table" );
    }

    public static String unStableFileName(int propertyId, long number)
    {
        return propertyId+"/"+makeFileName( number, "un", "table" );
    }

    public static String unPath(File proDir, long fileNumber) {
        return new File(proDir, unStableFileName(fileNumber)).getAbsolutePath();
    }

    public static String valIndexFileName(long fileId) {
        return makeFileName(fileId, "value", "index");
    }

    public static String aggrIndexFileName(long fileId) {
        return makeFileName(fileId, "aggr", "index");
    }



    /**
     * 返回对应编号的描述文件的名称，目前还没有用到。
     */
    @Deprecated
    public static String descriptorFileName(long number)
    {
        Preconditions.checkArgument(number >= 0, "number is negative");
        return String.format("MANIFEST-%06d", number);
    }

    /**
     * 返回CURRENT文件的名称，目前没有用到
     */
    @Deprecated
    public static String currentFileName()
    {
        return "CURRENT";
    }

    /**
     * 返回锁文件的名称
     */
    public static String lockFileName()
    {
        return "IS.RUNNING.LOCK";
    }

    /**
     * 返回临时文件的名称
     */
    public static String tempFileName(long number)
    {
        return makeFileName(number, "dbtmp");
    }

    /**
     * 返回LOG文件名称，目前没有用到
     */
    @Deprecated
    public static String infoLogFileName()
    {
        return "LOG";
    }

    public static String oldInfoLogFileName()
    {
        return "LOG.old";
    }

    /**
     * If filename is a leveldb file, store the type of the file in *type.
     * The number encoded in the filename is stored in *number.  If the
     * filename was successfully parsed, returns true.  Else return false.
     */
    public static FileInfo parseFileName(File file)
    {
        // Owned filenames have the form:
        //    dbname/CURRENT
        //    dbname/LOCK
        //    dbname/LOG
        //    dbname/LOG.old
        //    dbname/MANIFEST-[0-9]+
        //    dbname/[0-9]+.(log|sst|dbtmp)
        String fileName = file.getName();
        if ("CURRENT".equals(fileName)) {
            return new FileInfo(FileType.CURRENT);
        }
        else if ("LOCK".equals(fileName)) {
            return new FileInfo(FileType.DB_LOCK);
        }
        else if ("LOG".equals(fileName)) {
            return new FileInfo(FileType.INFO_LOG);
        }
        else if ("LOG.old".equals(fileName)) {
            return new FileInfo(FileType.INFO_LOG);
        }
        else if (fileName.startsWith("MANIFEST-")) {
            long fileNumber = Long.parseLong(removePrefix(fileName, "MANIFEST-"));
            return new FileInfo(FileType.DESCRIPTOR, fileNumber);
        }
        else if (fileName.endsWith(".log")) {
            long fileNumber = Long.parseLong(removeSuffix(fileName, ".log"));
            return new FileInfo(FileType.LOG, fileNumber);
        }
        else if (fileName.endsWith(".st")) {
            long fileNumber = Long.parseLong(removeSuffix(fileName, ".st"));
            return new FileInfo(FileType.STABLEFILE, fileNumber);
        }
        else if( fileName.endsWith( ".un" ) )
        {
            long fileNumber = Long.parseLong( removeSuffix( fileName, ".un" ) );
            return new FileInfo( FileType.UNSTABLEFILE, fileNumber );
        }
        else if (fileName.endsWith(".dbtmp")) {
            long fileNumber = Long.parseLong(removeSuffix(fileName, ".dbtmp"));
            return new FileInfo(FileType.TEMP, fileNumber);
        }
        else if(fileName.endsWith( ".buf" ))
        {
            long fileNumber = Long.parseLong(removeSuffix(fileName, ".buf"));
            return new FileInfo(FileType.BUFFER, fileNumber);
        }else if(fileName.endsWith( ".stbuf" ))
        {
            long fileNumber = Long.parseLong(removeSuffix(fileName, ".stbuf"));
            return new FileInfo(FileType.STBUFFER, fileNumber);
        }
        return null;
    }

    /**
     * Make the CURRENT file point to the descriptor file with the
     * specified number.
     *
     * @return true if successful; false otherwise
     */
    public static boolean setCurrentFile(File databaseDir, long descriptorNumber)
            throws IOException
    {
        String manifest = descriptorFileName(descriptorNumber);
        String temp = tempFileName(descriptorNumber);

        File tempFile = new File(databaseDir, temp);
        Files.write(manifest + "\n", tempFile, Charsets.UTF_8);
        File to = new File(databaseDir, currentFileName());
        boolean ok = tempFile.renameTo(to);
        if (!ok) {
            tempFile.delete();
            Files.write(manifest + "\n", to, Charsets.UTF_8);
        }
        return ok;
    }

    public static List<File> listFiles(File dir)
    {
        File[] files = dir.listFiles();
        if (files == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(files);
    }

    private static String makeFileName(long number, String suffix)
    {
        Preconditions.checkArgument(number >= 0, "number is negative");
        Preconditions.checkNotNull(suffix, "suffix is null");
        return String.format("%06d.%s", number, suffix);
    }

    private static String makeFileName(long number, String prefix, String suffix)
    {
        Preconditions.checkArgument(number >= 0, "number is negative");
        Preconditions.checkNotNull(suffix, "suffix is null");
        return String.format("%s.%06d.%s", prefix, number, suffix);
    }

    private static String removePrefix(String value, String prefix)
    {
        return value.substring(prefix.length());
    }

    private static String removeSuffix(String value, String suffix)
    {
        return value.substring(0, value.length() - suffix.length());
    }

    public static class FileInfo
    {
        private final FileType fileType;
        private final long fileNumber;

        public FileInfo(FileType fileType)
        {
            this(fileType, 0);
        }

        public FileInfo(FileType fileType, long fileNumber)
        {
            Preconditions.checkNotNull(fileType, "fileType is null");
            this.fileType = fileType;
            this.fileNumber = fileNumber;
        }

        public FileType getFileType()
        {
            return fileType;
        }

        public long getFileNumber()
        {
            return fileNumber;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FileInfo fileInfo = (FileInfo) o;

            if (fileNumber != fileInfo.fileNumber) {
                return false;
            }
            if (fileType != fileInfo.fileType) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = fileType.hashCode();
            result = 31 * result + (int) (fileNumber ^ (fileNumber >>> 32));
            return result;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("FileInfo");
            sb.append("{fileType=").append(fileType);
            sb.append(", fileNumber=").append(fileNumber);
            sb.append('}');
            return sb.toString();
        }
    }
}
