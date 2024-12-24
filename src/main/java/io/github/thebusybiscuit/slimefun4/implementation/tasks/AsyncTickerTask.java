package io.github.thebusybiscuit.slimefun4.implementation.tasks;

import com.xzavier0722.mc.plugin.slimefun4.storage.controller.SlimefunBlockData;
import com.xzavier0722.mc.plugin.slimefun4.storage.util.StorageCacheUtils;
import io.github.bakedlibs.dough.blocks.ChunkPosition;
import io.github.bakedlibs.dough.items.ItemStackSnapshot;
import io.github.thebusybiscuit.slimefun4.api.items.SlimefunItem;
import io.github.thebusybiscuit.slimefun4.implementation.Slimefun;
import me.mrCookieSlime.Slimefun.Objects.handlers.BlockTicker;
import org.bukkit.Location;
import org.bukkit.block.Block;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

public class AsyncTickerTask extends TickerTask{
    @Override
    public void run() {
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
            Set<BlockTicker> tickers = ConcurrentHashMap.<BlockTicker>newKeySet();

            // Run our ticker code
            if (!halted) {
                Set<ChunkPosition> loc;
                List<CompletableFuture<Void>> futures=new ArrayList<>();
                synchronized (tickingLocations) {
                    loc = new HashSet<>(tickingLocations.keySet());
                }

                for (ChunkPosition entry : loc) {
                    HashSet<Location> locs;
                    synchronized (tickingLocations) {
                        locs=new HashSet<>(tickingLocations.get(entry));
                    }
                    var tickThread=tickChunkAsync(entry,tickers,locs);
                    if(tickThread!=null) {
                        futures.add(tickThread);
                    }
                   // tickChunk(entry, tickers, new HashSet<>());
                }
                CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
            }

            // Start a new tick cycle for every BlockTicker
            for (BlockTicker ticker : tickers) {
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
    private CompletableFuture<Void> tickChunkAsync(ChunkPosition chunk,Set<BlockTicker> tickers,HashSet<Location> locations){
        try {
            // Only continue if the Chunk is actually loaded

            if (chunk.isLoaded()) {
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {});
                for (Location l : locations) {
                    future=tickLocationAsync(tickers, l,future);
                }
                return future;
            }

        } catch (ArrayIndexOutOfBoundsException | NumberFormatException x) {
            Slimefun.logger()
                .log(Level.SEVERE, x, () -> "An Exception has occurred while trying to resolve Chunk: " + chunk);
        }
        return null;
    }
    private CompletableFuture<Void> tickLocationAsync(@Nonnull Set<BlockTicker> tickers, @Nonnull Location l,CompletableFuture<Void> future) {
        var blockData = StorageCacheUtils.getBlock(l);
        if (blockData == null || !blockData.isDataLoaded() || blockData.isPendingRemove()) {
            return future;
        }
        SlimefunItem item = SlimefunItem.getById(blockData.getSfId());

        if (item != null && item.getBlockTicker() != null) {
            if (item.isDisabledIn(l.getWorld())) {
                return future;
            }
            //future=future.thenRun(()->{
            try {
                BlockTicker ticker = item.getBlockTicker();
                tickers.add(ticker);
                if (ticker.isSynchronized()) {
                    Slimefun.getProfiler().scheduleEntries(1);
                    synchronized (ticker){
                        ticker.update();
                    }
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
                    future.thenRun(()->{
                        try{
                            long timestamp = Slimefun.getProfiler().newEntry();
                            Block b = l.getBlock();
                            synchronized (ticker){
                                ticker.update();
                                tickBlock(l, b, item, blockData, timestamp);
                            }

                        }catch(Throwable ignored){}
                    });
                }
            } catch (Throwable x) {
                reportErrors(l, item, x);
            }
        }
        return future;
    }


}
