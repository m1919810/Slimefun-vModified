package com.xzavier0722.mc.plugin.slimefun4.storage.controller;

import city.norain.slimefun4.api.menu.UniversalMenu;
import city.norain.slimefun4.utils.ClassUtil;
import com.xzavier0722.mc.plugin.slimefun4.storage.controller.attributes.UniversalDataTrait;
import io.github.thebusybiscuit.slimefun4.implementation.Slimefun;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import lombok.Getter;
import lombok.Setter;
import org.bukkit.inventory.ItemStack;

@Getter
public class SlimefunUniversalData extends ASlimefunDataContainer {
    @Setter
    private volatile UniversalMenu menu;

    @Setter
    private volatile boolean pendingRemove = false;

    private final Set<UniversalDataTrait> traits = new HashSet<>();

    @ParametersAreNonnullByDefault
    SlimefunUniversalData(UUID uuid, String sfId) {
        super(uuid.toString(), sfId);
    }

    @ParametersAreNonnullByDefault
    public void setData(String key, String val) {
        checkData();

        if (UniversalDataTrait.isReservedKey(key)) {
            var caller = ClassUtil.getCallerClass();

            if (!caller.startsWith("com.xzavier0722.mc.plugin.slimefun4.storage.controller")) {
                throw new RuntimeException("You cannot set data for reserved key!");
            }
        }

        setCacheInternal(key, val, true);
        Slimefun.getDatabaseManager().getBlockDataController().scheduleDelayedUniversalDataUpdate(this, key);
    }

    @ParametersAreNonnullByDefault
    public void removeData(String key) {
        if (removeCacheInternal(key) != null || !isDataLoaded()) {
            Slimefun.getDatabaseManager().getBlockDataController().scheduleDelayedUniversalDataUpdate(this, key);
        }
    }

    @Nullable public ItemStack[] getMenuContents() {
        if (menu == null) {
            return null;
        }
        var re = new ItemStack[54];
        var presetSlots = menu.getPreset().getPresetSlots();
        var inv = menu.toInventory().getContents();
        for (var i = 0; i < inv.length; i++) {
            if (presetSlots.contains(i)) {
                continue;
            }
            re[i] = inv[i];
        }

        return re;
    }

    public UUID getUUID() {
        return UUID.fromString(getKey());
    }

    public boolean hasTrait(UniversalDataTrait trait) {
        return traits.contains(trait);
    }

    @Override
    public String toString() {
        return "SlimefunUniversalData [uuid= " + getUUID() + ", sfId=" + getSfId() + ", isPendingRemove="
                + pendingRemove + "]";
    }
}
