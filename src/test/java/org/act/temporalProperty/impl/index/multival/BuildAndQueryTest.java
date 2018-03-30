package org.act.temporalProperty.impl.index.multival;

import org.act.temporalProperty.TemporalPropertyStore;
import org.act.temporalProperty.impl.RangeQueryCallBack;
import org.act.temporalProperty.index.IndexQueryRegion;
import org.act.temporalProperty.index.IndexValueType;
import org.act.temporalProperty.index.PropertyValueInterval;
import org.act.temporalProperty.index.rtree.IndexEntry;
import org.act.temporalProperty.util.Slice;
import org.act.temporalProperty.util.StoreBuilder;
import org.act.temporalProperty.util.TrafficDataImporter;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by song on 2018-01-22.
 */
public class BuildAndQueryTest {
    private static Logger log = LoggerFactory.getLogger(BuildAndQueryTest.class);

    private static String dataPath = "/home/song/tmp/road data";
    private static String dbDir = "/tmp/temporal.property.test";
    private static TemporalPropertyStore store;
    private static StoreBuilder stBuilder;
    private static TrafficDataImporter importer;

    @BeforeClass
    public static void initDB() throws Throwable {
        stBuilder = new StoreBuilder(dbDir, true);
        importer = new TrafficDataImporter(stBuilder.store(), dataPath, 100);
        log.info("time: {} - {}", importer.getMinTime(), importer.getMaxTime());
        store = stBuilder.store();
    }

    @Before
    public void buildIndex(){
        List<Integer> proIds = new ArrayList<>();
        proIds.add(1);
        proIds.add(2);
        proIds.add(3);
        proIds.add(4);
//        store.createValueIndex(1288803660, 1288824660, proIds, types);
//        store.createValueIndex(1288800300, 1288802460, proIds, types);
        store.createValueIndex(1560, 27360, proIds);
        log.info("create index done");
    }

    @Test
    public void main() throws Throwable {
//        testRangeQuery(store);
//        List<Long> iterResult = queryByIter( 18300, 27000, 0, 200);

        List<IndexEntry> indexResult = queryByIndex(18300, 27000, 0, 200);
        for(int i=0; i<100; i++){
            log.debug("{}", indexResult.get(i));
        }

//        for(int time=0; time<=18300; time+=100){
//            for(int value=0; value<=400; value+=20){
//
//            }
//        }
        store.shutDown();
    }

    private List<IndexEntry> queryByIndex(int timeMin, int timeMax, int valueMin, int valueMax){
        IndexQueryRegion condition = new IndexQueryRegion(timeMin, timeMax);
        Slice minValue = new Slice(4);
        minValue.setInt(0, valueMin);
        Slice maxValue = new Slice(4);
        maxValue.setInt(0, valueMax);
        condition.add(new PropertyValueInterval(1, minValue, maxValue, IndexValueType.INT));
        List<IndexEntry> result = store.getEntries(condition);
        log.info("index result count {}", result.size());
        return result;
    }

    private List<Long> queryByIter(int timeMin, int timeMax, int valueMin, int valueMax){
        List<Long> result = new ArrayList<>();
        for(Long entityId : importer.getRoadIdMap().values()){
            try {
                store.getRangeValue(entityId, 1, timeMin, timeMax, new EntityIdCallBack(timeMin, timeMax, valueMin, valueMax));
            }catch (StopLoopException e){
                result.add(entityId);
            }catch (RuntimeException e){
                log.debug("entity id: {} {}", entityId, e.getMessage());
                result.add(entityId);
            }
        }
        log.info("iterate result count {}", result.size());
        return result;
    }

    @After
    public void closeDB(){
        if(store!=null) store.shutDown();
    }



    private static void testRangeQuery(TemporalPropertyStore store) {
        store.getRangeValue(2, 1, 1560, 27000, new RangeQueryCallBack() {
            public void setValueType(String valueType) {}
            public void onCall(int time, Slice value) {
                log.info("{} {}", time, value.getInt(0));
            }
            public void onCallBatch(Slice batchValue){}
            public Object onReturn(){return null;}
            public CallBackType getType(){return null;}
        });
    }



    private class EntityIdCallBack extends RangeQueryCallBack {
        private int timeMin, timeMax, valueMin, valueMax, lastTime = -1;
        private boolean first = true;

        public EntityIdCallBack(int timeMin, int timeMax, int valueMin, int valueMax) {
            this.timeMin = timeMin;
            this.timeMax = timeMax;
            this.valueMin = valueMin;
            this.valueMax = valueMax;
        }

        private boolean overlap(int t1min, int t1max, int t2min, int t2max){
            return (t1min<=t2max && t2min<=t1max);
        }

        public void onCall(int time, Slice value) {
            int val = value.getInt(0);
            if(first){
                first=false;
            }else{
                if(lastTime>=time){
                    log.trace("time not inc: last("+lastTime+") cur("+time+")");
//                    throw new RuntimeException("time not inc: last("+lastTime+") cur("+time+")");
                }
                if(overlap(lastTime, time, timeMin, timeMax) &&
                        (valueMin <= val && val <= valueMax)){
                    throw new StopLoopException();
                }
            }
            lastTime = time;
        }
        public void setValueType(String valueType) {}
        public void onCallBatch(Slice batchValue) {}
        public Object onReturn() {return null;}
        public CallBackType getType() {return null;}
    };

    private class StopLoopException extends RuntimeException {
    }
}