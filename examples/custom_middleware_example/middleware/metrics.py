from fastsqs import Middleware
import time

class MetricsMiddleware(Middleware):
    async def before(self, payload, record, context, ctx):
        if "metrics" not in ctx.state:
            ctx.state["metrics"] = {
                "total_messages": 0,
                "successful_messages": 0,
                "failed_messages": 0,
                "processing_times": [],
            }
        ctx.state["metrics"]["total_messages"] += 1
        ctx.state["start_time"] = time.time()

    async def after(self, payload, record, context, ctx, error):
        metrics = ctx.state.get("metrics", {})
        if error is not None:
            metrics["failed_messages"] += 1
            print("\n=== METRICS SUMMARY ===")
            print(f"Total messages: {metrics['total_messages']}")
            print(f"Successful: {metrics['successful_messages']}")
            print(f"Failed: {metrics['failed_messages']}")
            success_rate = metrics["successful_messages"] / metrics["total_messages"] * 100
            print(f"Success rate: {success_rate:.1f}%")
            if metrics["processing_times"]:
                avg_time = sum(metrics["processing_times"]) / len(metrics["processing_times"])
                print(f"Average processing time: {avg_time:.3f}s")
        else:
            end_time = time.time()
            start_time = ctx.state.get("start_time", end_time)
            duration = end_time - start_time
            metrics["successful_messages"] += 1
            metrics["processing_times"].append(duration)
