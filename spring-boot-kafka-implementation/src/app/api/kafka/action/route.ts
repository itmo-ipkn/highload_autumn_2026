import { NextRequest, NextResponse } from "next/server";
import {
  generateVerifyReport,
  getDashboardSnapshot,
  runFailureExperiment,
  runPipelineBatch,
} from "@/lib/kafka-engine";

export async function POST(req: NextRequest) {
  try {
    const body = await req.json();
    const { action, batchSize, githubLogin } = body;

    if (action === "produce_batch") {
      await runPipelineBatch(Number(batchSize) || 4);
    } else if (action === "run_verify") {
      await generateVerifyReport(githubLogin);
    } else if (
      [
        "exp1_kill_consumer2",
        "exp1_restart_consumer2",
        "exp2_stop_broker1",
        "exp2_stop_broker2",
        "exp2_recover_brokers",
        "exp3_key_mode_none",
        "exp3_key_mode_user_id",
        "sigterm_flush_producers",
        "reset_clean_baseline",
      ].includes(action)
    ) {
      await runFailureExperiment(action);
    } else {
      return NextResponse.json(
        { ok: false, error: `Unknown action: ${action}` },
        { status: 400 }
      );
    }

    const snapshot = await getDashboardSnapshot();
    return NextResponse.json({
      ok: true,
      ...snapshot,
    });
  } catch (error: any) {
    console.error("Failed to execute Kafka action:", error);
    return NextResponse.json(
      { ok: false, error: error?.message || "Failed to execute Kafka action" },
      { status: 500 }
    );
  }
}
