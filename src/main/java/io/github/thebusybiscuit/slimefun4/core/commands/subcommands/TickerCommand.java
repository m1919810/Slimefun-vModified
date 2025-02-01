package io.github.thebusybiscuit.slimefun4.core.commands.subcommands;

import com.google.common.base.Preconditions;
import io.github.thebusybiscuit.slimefun4.core.commands.SlimefunCommand;
import io.github.thebusybiscuit.slimefun4.core.commands.SubCommand;
import io.github.thebusybiscuit.slimefun4.implementation.Slimefun;
import io.github.thebusybiscuit.slimefun4.implementation.tasks.AsyncTickerTask;
import java.util.Arrays;
import javax.annotation.Nonnull;
import org.bukkit.command.CommandSender;

public class TickerCommand extends SubCommand {
    protected TickerCommand(Slimefun plugin, SlimefunCommand cmd) {
        super(plugin, cmd, "ticker", true);
    }

    @Override
    public void onExecute(@Nonnull CommandSender sender, @Nonnull String[] args) {
        if (sender.hasPermission("slimefun.command.debug")) {
            if (args.length >= 3) {
                Slimefun.logger().info(Arrays.stream(args).toList().toString());
                switch (args[1]) {
                    case "toggleAsync":
                        var ticker = ((AsyncTickerTask) Slimefun.getTickerTask());
                        ticker.setUseAsync(Boolean.parseBoolean(args[2]));
                        sender.sendMessage("toggle Async Ticker " + Boolean.toString(ticker.isUseAsync()));
                        break;
                    case "tickRate":
                        int rate = Integer.parseInt(args[2]);
                        Preconditions.checkArgument(rate >= 1, "tick rate larger than 0");
                        var ticker1 = Slimefun.getTickerTask();
                        ticker1.setTickRate(rate);
                        ticker1.restart(Slimefun.instance());
                        sender.sendMessage("restart ticker with tickRate " + rate);
                        break;
                    case "parallelism":
                        int parallelism = Integer.parseInt(args[2]);
                        Preconditions.checkArgument(parallelism >= 1, "parallelism larger than 0");
                        var ticker2 = (AsyncTickerTask) Slimefun.getTickerTask();
                        ticker2.setThreadCount(parallelism);
                        ticker2.resetTheadPool();
                        sender.sendMessage("restart thread pool with parallelism " + parallelism);
                        break;
                    case "pool":
                        var ticker3 = (AsyncTickerTask) Slimefun.getTickerTask();
                        ticker3.setPoolType(args[2]);
                        ticker3.resetTheadPool();
                        sender.sendMessage("restart thread pool with poolType " + args[2]);
                        break;
                    default:
                        sender.sendMessage("Unknown command");
                }
            }
        } else {
            Slimefun.getLocalization().sendMessage(sender, "messages.no-permission", true);
        }
    }
}
