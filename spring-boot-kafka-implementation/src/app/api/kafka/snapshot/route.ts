import { NextResponse } from "next/server";
import { getDashboardSnapshot } from "@/lib/kafka-engine";
import { getAllDeliverableFiles } from "@/lib/project-files";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const [snapshot, deliverableFiles] = await Promise.all([
      getDashboardSnapshot(),
      getAllDeliverableFiles(),
    ]);

    return NextResponse.json({
      ok: true,
      ...snapshot,
      deliverableFiles,
    });
  } catch (error: any) {
    console.error("Failed to load Kafka snapshot:", error);
    return NextResponse.json(
      { ok: false, error: error?.message || "Failed to load Kafka snapshot" },
      { status: 500 }
    );
  }
}
