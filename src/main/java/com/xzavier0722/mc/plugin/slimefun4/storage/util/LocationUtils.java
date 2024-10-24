package com.xzavier0722.mc.plugin.slimefun4.storage.util;

import city.norain.slimefun4.SlimefunExtended;
import org.bukkit.Bukkit;
import org.bukkit.Chunk;
import org.bukkit.Location;
import org.bukkit.World;

public class LocationUtils {
    public static String getLocKey(Location l) {
        return l.getWorld().getName() + ";" + l.getBlockX() + ":" + l.getBlockY() + ":" + l.getBlockZ();
    }

    public static String getChunkKey(Chunk chunk) {
        return chunk.getWorld().getName() + ";" + chunk.getX() + ":" + chunk.getZ();
    }
    public static String getChunkKey(Location loc) {
        return loc.getWorld().getName() + ";" + (loc.getBlockX()>>4) + ":" + (loc.getBlockZ()>>4);
    }

    public static Location toLocation(String lKey) {
        var strArr = lKey.split(";");
        var loc = strArr[1].split(":");
        return new Location(
                Bukkit.getWorld(strArr[0]),
                Double.parseDouble(loc[0]),
                Double.parseDouble(loc[1]),
                Double.parseDouble(loc[2]));
    }

    public static boolean isSameChunk(Chunk c1, Chunk c2) {
        return c1 == c2
                || (isSameWorld(c1.getWorld(), c2.getWorld()) && c1.getX() == c2.getX() && c1.getZ() == c2.getZ());
    }

    public static boolean isSameLoc(Location l1, Location l2) {
        return l1 == l2
                || (isSameChunk(l1.getChunk(), l2.getChunk())
                        && l1.getBlockX() == l2.getBlockX()
                        && l1.getBlockY() == l2.getBlockY()
                        && l1.getBlockZ() == l2.getBlockZ());
    }

    public static Chunk toChunk(World w, String cKey) {
        var loc = cKey.split(";")[1].split(":");
        if (SlimefunExtended.getMinecraftVersion().isAtLeast(1, 19, 4)) {
            return w.getChunkAt(Integer.parseInt(loc[0]), Integer.parseInt(loc[1]), false);
        } else {
            return w.getChunkAt(Integer.parseInt(loc[0]), Integer.parseInt(loc[1]));
        }
    }

    public static boolean isSameWorld(World w1, World w2) {
        return w1.getName().equals(w2.getName());
    }

    public static String locationToString(Location location) {
        if (location == null) {
            return "";
        }

        return "[world="
                + location.getWorld().getName()
                + ",x="
                + location.getX()
                + ",y="
                + location.getY()
                + ",z="
                + location.getZ()
                + "]";
    }
}
