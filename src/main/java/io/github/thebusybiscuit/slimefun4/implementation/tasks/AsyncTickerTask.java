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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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

                for (ChunkPosition entry : loc) {
                    HashSet<Location> locs;
                    synchronized (tickingLocations) {
                        locs=new HashSet<>(tickingLocations.get(entry));
                    }
                    tickChunkAsync(entry,tickers,syncTickers,locs);

                }
                CompletableFuture.allOf(tickers.values().toArray(CompletableFuture[]::new)).orTimeout(10, TimeUnit.SECONDS).exceptionally(ex -> {
                    // 超时或者其他异常的处理逻辑
                    Slimefun.logger()
                        .log(Level.SEVERE,()->{return "Timeout or error occurred in AsyncTickTask: " + ex.getMessage();});
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
    private void tickChunkAsync(ChunkPosition chunk,HashMap<BlockTicker,CompletableFuture<Void>> tickers,HashSet<BlockTicker> syncTickers,HashSet<Location> locations){
        try {
            // Only continue if the Chunk is actually loaded

            if (chunk.isLoaded()) {
                for (Location l : locations) {
                    tickLocationAsync(tickers,syncTickers, l);
                }
            }

        } catch (ArrayIndexOutOfBoundsException | NumberFormatException x) {
            Slimefun.logger()
                .log(Level.SEVERE, x, () -> "An Exception has occurred while trying to resolve Chunk: " + chunk);
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
                        return future==null?CompletableFuture.runAsync(tickTask):future.thenRun(tickTask);
                    });
                }
            } catch (Throwable x) {
                reportErrors(l, item, x);
            }
        }
    }


}
