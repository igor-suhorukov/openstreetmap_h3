package com.github.isuhorukov.osm.pgsnapshot.util;

public class CompactH3 {

    public static long toFull3(long highSh) {
        highSh &=0x000000000000ffff;
        highSh <<=36;
        highSh |= 0x830000fffffffffL;
        return highSh;
    }

    public static long toShort3(long highSh) {
        highSh &= 0x00ffff000000000L;
        highSh >>>=36;
        return highSh;
    }

    public static long toShort8(long highSh) {
        highSh &= 0x00ffffffff00000L;
        highSh >>>=20;
        /*
            8800000001fffff
         */
        return highSh;
    }
    /*

select (590282369378811903 & 'x000ffffffff00000'::bit(64)::bigint>>20)::integer
     */

    public static long toFull8(long highSh) {
        //highSh &=0x00000000ffffffff;
        highSh <<=20;
        highSh |= 0x8800000000fffffL;
        return highSh;
    }


    public static short serializeShort(long h33val){
        return (short) h33val;
    }

    public static int serializeInt(long h38val){
        return (int) h38val;
    }

    public static int serialize8(long h3Cell8) {
        return CompactH3.serializeInt(CompactH3.toShort8(h3Cell8));
    }

    public static short serialize3(long h3Cell3) {
        return CompactH3.serializeShort(CompactH3.toShort3(h3Cell3));
    }
}
