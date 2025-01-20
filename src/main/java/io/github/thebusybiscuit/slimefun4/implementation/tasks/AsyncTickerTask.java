package io.github.thebusybiscuit.slimefun4.implementation.tasks;

import com.xzavier0722.mc.plugin.slimefun4.storage.controller.SlimefunBlockData;
import com.xzavier0722.mc.plugin.slimefun4.storage.util.StorageCacheUtils;
import io.github.bakedlibs.dough.blocks.ChunkPosition;
import io.github.bakedlibs.dough.items.ItemStackSnapshot;
import io.github.thebusybiscuit.slimefun4.api.items.SlimefunItem;
import io.github.thebusybiscuit.slimefun4.implementation.Slimefun;
import lombok.Getter;
import lombok.Setter;
import me.mrCookieSlime.Slimefun.Objects.handlers.BlockTicker;
import org.bukkit.Location;
import org.bukkit.block.Block;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class AsyncTickerTask extends TickerTask{
    @Setter
    @Getter
    private boolean useAsync=Slimefun.getCfg().getOrSetDefault("URID.enable-async-tickers",true);
    @Setter
    private int threadCount = (Math.min(32767, Runtime.getRuntime().availableProcessors())/2)+4;
    @Setter
    private String poolType = "ForkJoinPool"; // "ThreadPoolExecutor";
    //we figured out that ForkJoinPool performs better in my structure
    //in ThreadPoolExecutor,one task costs more time to complete
    //I don't know why
    private AbstractExecutorService tickerThreadPool;
    public AsyncTickerTask(){
        Slimefun.logger().log(Level.INFO,"Setting up tick task");
        if(useAsync){
            Slimefun.logger().log(Level.INFO,"Async Ticker enabled");
            resetTheadPool();
        }else {
            Slimefun.logger().log(Level.INFO,"Async Ticker disabled");
        }
        
    }
    public synchronized void resetTheadPool(){
        if(tickerThreadPool!=null){
            tickerThreadPool.shutdown();
        }
        tickerThreadPool = genPool();

    }
    public AbstractExecutorService genPool(){
        AbstractExecutorService result;
       if("ThreadPoolExecutor".equals(poolType)){
           var re= new ThreadPoolExecutor(threadCount,2*threadCount-2,60,TimeUnit.SECONDS,
               new ArrayBlockingQueue<>(50000),new ThreadPoolExecutor.CallerRunsPolicy());
           Slimefun.logger().log(Level.INFO,"Starting ticker ThreadPoolExecutor with core pool size "+re.getCorePoolSize()+" and max pool size "+re.getMaximumPoolSize());
           result = re;
       }else if("ForkJoinPool".equals(poolType)){
           var re= new ForkJoinPool(threadCount);
           Slimefun.logger().log(Level.INFO,"Starting ticker ForkJoinPool with parallelism "+re.getParallelism());
           result = re;
       }
       else {
           result = ForkJoinPool.commonPool();
           Slimefun.logger().log(Level.INFO,"Starting ticker using common ForkJoinPool");
       }
       return result;
    }
    public AbstractExecutorService getTickerThreadPool(){
        if(tickerThreadPool==null){
            resetTheadPool();
        }
        return tickerThreadPool;
    }
    @Override
    public void run() {
        if(!useAsync){
            super.run();
            return;
        }
        if (paused) {
            return;
        }
        try {
            // If this method is actually still running... DON'T
            if (running) {
                return;
            }
            running = true;
            Slimefun.getProfiler().start();
            //each ticker map to a single task chain ,in case of async invoke which led to inner ConcurrentModificationException
            HashMap<BlockTicker,CompletableFuture<Void>> tickers = new HashMap<>();
            //sync tickers are rare
            HashSet<BlockTicker> syncTickers = new HashSet<>();
            // Run our ticker code
            if (!halted) {
                Set<ChunkPosition> loc;
                synchronized (tickingLocations) {
                    loc = new HashSet<>(tickingLocations.keySet());
                }
                Map<ChunkPosition, Iterator<Location>> chunkMachineSequence = new HashMap<>();
                for (ChunkPosition entry : loc) {
                    try{
                        if(entry.isLoaded()){
                            HashSet<Location> locs;
                            synchronized (tickingLocations) {
                                locs=new HashSet<>(tickingLocations.get(entry));
                            }
                            chunkMachineSequence.put(entry, locs.iterator());
                        }
                    } catch (ArrayIndexOutOfBoundsException | NumberFormatException x) {
                        Slimefun.logger().log(Level.SEVERE, x, () -> "An Exception has occurred while trying to resolve Chunk: " + entry);
                    }
                }
                tickChunkAsync(chunkMachineSequence,tickers,syncTickers);
                CompletableFuture.allOf(tickers.values().toArray(CompletableFuture[]::new)).orTimeout(10, TimeUnit.SECONDS).exceptionally(ex -> {
                    // 超时或者其他异常的处理逻辑
                    Slimefun.logger().log(Level.SEVERE,ex,()->{return "Timeout or error occurred in AsyncTickTask: ";});
                    Slimefun.logger().log(Level.WARNING,()->{return "Resetting Thread Pool... ";});
                    if(!this.halted &&!this.paused){
                        resetTheadPool();
                    }

                    return null;
                }).join();
            }

            // Start a new tick cycle for every BlockTicker
            for (BlockTicker ticker : tickers.keySet()) {
                ticker.startNewTick();
            }
            for (BlockTicker ticker : syncTickers) {
                ticker.startNewTick();
            }
            Slimefun.getProfiler().stop();
        } catch (Exception | LinkageError x) {
            Slimefun.logger()
                .log(
                    Level.SEVERE,
                    x,
                    () -> "An Exception was caught while ticking the Block Tickers Task for Slimefun v"
                        + Slimefun.getVersion());
            //reset();
        }finally {
            reset();
        }
    }
    //todo we can reSchedule chunk execute order
    //todo for example : each chunk launch first machine task, then another,then another
    //this may help decrease concurrent errors due to locality,and it also keeps a chunk's task-launching order
    private void tickChunkAsync(Map<ChunkPosition, Iterator<Location>> machineSequence,HashMap<BlockTicker,CompletableFuture<Void>> tickers,HashSet<BlockTicker> syncTickers){
        while(!machineSequence.isEmpty()){
            var iter = machineSequence.entrySet().iterator();
            while(iter.hasNext()){
                var entry = iter.next();
                var locationIter = entry.getValue();
                if(locationIter.hasNext()){
                    Location location = locationIter.next();
                    tickLocationAsync(tickers,syncTickers,location);
                }else {
                    iter.remove();
                }
            }
        }
    }
    private void tickLocationAsync(@Nonnull HashMap<BlockTicker,CompletableFuture<Void>> tickers,HashSet<BlockTicker> syncticker, @Nonnull Location l) {
        var blockData = StorageCacheUtils.getBlock(l);
        if (blockData == null || !blockData.isDataLoaded() || blockData.isPendingRemove()) {
            return ;
        }
        SlimefunItem item = SlimefunItem.getById(blockData.getSfId());

        if (item != null && item.getBlockTicker() != null) {
            if (item.isDisabledIn(l.getWorld())) {
                return ;
            }
            try {
                BlockTicker ticker = item.getBlockTicker();
                ticker.update();
                if (ticker.isSynchronized()) {
                    syncticker.add(ticker);
                    Slimefun.getProfiler().scheduleEntries(1);
                    /**
                     * We are inserting a new timestamp because synchronized actions
                     * are always ran with a 50ms delay (1 game tick)
                     */
                    Slimefun.runSync(() -> {
                        if (blockData.isPendingRemove()) {
                            return;
                        }
                        Block b = l.getBlock();
                        tickBlock(l, b, item, blockData, System.nanoTime());
                    });
                } else {
                    tickers.compute(ticker,(bt,future)->{
                        Runnable tickTask=()->{
                            long timestamp = Slimefun.getProfiler().newEntry();
                            try {
                                Block b = l.getBlock();
                                ticker.tick(b, item, blockData);
                            } catch (Throwable x) {
                                reportErrors(l, item, x);
                            } finally {
                                Slimefun.getProfiler().closeEntry(l, item, timestamp);
                            }
                        };
                        return future==null?CompletableFuture.runAsync(tickTask,getTickerThreadPool()).exceptionally((ignored)->null):future.thenRunAsync(tickTask,getTickerThreadPool()).exceptionally((ignored)->null);
                    });
                }
            } catch (Throwable x) {
                reportErrors(l, item, x);
            }
        }
    }


}
