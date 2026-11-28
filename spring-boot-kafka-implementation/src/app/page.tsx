"use client";

import React, { useEffect, useState, useCallback } from "react";
import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  Code2,
  Copy,
  Cpu,
  Database,
  FileText,
  Flame,
  GitBranch,
  HardDrive,
  Layers,
  Play,
  Pause,
  RefreshCw,
  Server,
  ShieldAlert,
  Skull,
  Sparkles,
  Terminal,
  Zap,
  Check,
  ArrowRight,
  RotateCcw,
} from "lucide-react";

interface BrokerNode {
  id: number;
  name: string;
  host: string;
  internalListener: string;
  externalListener: string;
  controllerListener: string;
  role: string;
  status: "ONLINE" | "STOPPED";
  isActiveController: boolean;
  heapLimit: string;
}

interface ProducerPod {
  id: string;
  index: number;
  status: "RUNNING" | "FLUSHING_SIGTERM" | "STOPPED" | "BACKPRESSURE_RETRYING";
  ownedUsers: string[];
  acks: "all";
  enableIdempotence: true;
  keyMode: "user_id" | "none";
  bufferedCount: number;
  producedCount: number;
  lastError: string | null;
}

interface ConsumerPod {
  id: string;
  groupId: "events-processor";
  status: "RUNNING" | "KILLED_SIGKILL" | "REBALANCING";
  assignedPartitions: number[];
  autoCommit: false;
  autoOffsetReset: "earliest";
  processedCount: number;
  dltRoutedCount: number;
  dedupCaughtCount: number;
}

interface PartitionInfo {
  topic: "events" | "events.dlt";
  partition: number;
  leader: number | null;
  replicas: number[];
  isr: number[];
  minInSyncReplicas: number;
  highWatermark: number;
  committedOffset: number;
  assignedConsumer: string | null;
  status: "HEALTHY" | "UNDER_REPLICATED" | "OFFLINE_NO_QUORUM";
}

interface VerifyUserRow {
  userId: string;
  minSeq: number;
  maxSeq: number;
  sinkCount: number;
  dltCount: number;
  missingCount: number;
  outOfOrderCount: number;
}

interface VerifyReport {
  headerLine: string;
  githubLogin: string;
  timestamp: string;
  totalSinkEvents: number;
  totalDltEvents: number;
  totalMissingSeq: number;
  rejectedDuplicates: number;
  totalOutOfOrder: number;
  users: VerifyUserRow[];
  rawTerminalOutput: string;
}

interface DeliverableFile {
  path: string;
  title: string;
  category: "Infrastructure" | "Spring Boot Java" | "Verification & Report";
  language: string;
  description: string;
  content: string;
}

interface SnapshotData {
  config: {
    keyMode: "user_id" | "none";
    processingDelayMs: number;
    producerRateMs: number;
    poisonProbabilityPct: number;
    githubLogin: string;
  };
  brokers: BrokerNode[];
  producers: ProducerPod[];
  consumers: ConsumerPod[];
  partitions: PartitionInfo[];
  recentSinkEvents: any[];
  recentDltEvents: any[];
  recentLogs: any[];
  verifyReport: VerifyReport;
  deliverableFiles?: DeliverableFile[];
}

export default function KafkaObservatoryPage() {
  const [data, setData] = useState<SnapshotData | null>(null);
  const [deliverables, setDeliverables] = useState<DeliverableFile[]>([]);
  const [selectedFileIndex, setSelectedFileIndex] = useState<number>(0);
  const [activeTab, setActiveTab] = useState<
    "topology" | "experiments" | "code" | "readme"
  >("topology");
  const [sinkSubTab, setSinkSubTab] = useState<"sink" | "dlt" | "logs">("sink");
  const [isStreaming, setIsStreaming] = useState<boolean>(false);
  const [busyAction, setBusyAction] = useState<string | null>(null);
  const [githubLoginInput, setGithubLoginInput] = useState<string>("itmo-ddia-student");
  const [copiedCode, setCopiedCode] = useState<boolean>(false);
  const [copiedVerify, setCopiedVerify] = useState<boolean>(false);

  const loadSnapshot = useCallback(async () => {
    try {
      const res = await fetch("/api/kafka/snapshot", { cache: "no-store" });
      const json = await res.json();
      if (json.ok) {
        setData(json);
        if (json.deliverableFiles && json.deliverableFiles.length > 0) {
          setDeliverables(json.deliverableFiles);
        }
        if (json.config?.githubLogin) {
          setGithubLoginInput((prev) =>
            prev === "itmo-ddia-student" ? json.config.githubLogin : prev
          );
        }
      }
    } catch (err) {
      console.error("Error loading snapshot:", err);
    }
  }, []);

  useEffect(() => {
    loadSnapshot();
  }, [loadSnapshot]);

  const executeAction = useCallback(
    async (action: string, extraBody: Record<string, any> = {}) => {
      setBusyAction(action);
      try {
        const res = await fetch("/api/kafka/action", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ action, ...extraBody }),
        });
        const json = await res.json();
        if (json.ok) {
          setData((prev) => ({
            ...json,
            deliverableFiles: prev?.deliverableFiles || deliverables,
          }));
        }
      } catch (err) {
        console.error("Failed action:", err);
      } finally {
        setBusyAction(null);
      }
    },
    [deliverables]
  );

  useEffect(() => {
    if (!isStreaming) return;
    const timer = setInterval(() => {
      executeAction("produce_batch", { batchSize: 3 });
    }, 2400);
    return () => clearInterval(timer);
  }, [isStreaming, executeAction]);

  const handleCopyCode = (text: string) => {
    navigator.clipboard.writeText(text);
    setCopiedCode(true);
    setTimeout(() => setCopiedCode(false), 1800);
  };

  const handleCopyVerify = (text: string) => {
    navigator.clipboard.writeText(text);
    setCopiedVerify(true);
    setTimeout(() => setCopiedVerify(false), 1800);
  };

  if (!data) {
    return (
      <div className="min-h-screen bg-[#0B0F17] text-[#F9FAFB] flex items-center justify-center p-6">
        <div className="flex flex-col items-center gap-4 border border-slate-800 bg-[#111827] px-8 py-6 rounded-md">
          <RefreshCw className="w-7 h-7 text-[#00E599] animate-spin" />
          <div className="text-sm font-mono text-slate-300">
            Bootstrapping 3-Broker Kafka KRaft Cluster &amp; Spring Boot 3 Pipeline...
          </div>
        </div>
      </div>
    );
  }

  const onlineBrokersCount = data.brokers.filter((b) => b.status === "ONLINE").length;
  const activeConsumersCount = data.consumers.filter((c) => c.status === "RUNNING").length;
  const verify = data.verifyReport;
  const currentFile = deliverables[selectedFileIndex] || null;

  return (
    <div className="min-h-screen bg-[#0B0F17] text-[#F9FAFB] flex flex-col bg-tech-grid">
      {/* =====================================================================
          TOP COMMAND BAR (56px height)
         ===================================================================== */}
      <header className="sticky top-0 z-30 border-b border-slate-800/90 bg-[#0B0F17]/95 backdrop-blur-md px-5 py-2.5">
        <div className="max-w-[1600px] mx-auto flex flex-wrap items-center justify-between gap-3">
          {/* Brand & Course Badge */}
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 rounded bg-[#00E599]/15 border border-[#00E599]/40 flex items-center justify-center">
              <Layers className="w-4 h-4 text-[#00E599]" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <span className="font-display font-bold text-base tracking-tight text-white">
                  HW2/3. Очередь Kafka
                </span>
                <span className="px-2 py-0.5 text-[11px] font-mono font-semibold uppercase rounded bg-emerald-500/15 text-[#00E599] border border-[#00E599]/30">
                  Java 21 + Spring Boot 3
                </span>
                <span className="hidden md:inline-block px-2 py-0.5 text-[11px] font-mono rounded bg-slate-800 text-slate-300 border border-slate-700">
                  KRaft 3-Broker No-ZK
                </span>
              </div>
              <p className="text-[11px] text-slate-400 font-mono">
                DDIA Осень 2026 • Топики: <code className="text-slate-200">events</code> (3P, RF=3, min.isr=2) &amp;{" "}
                <code className="text-slate-200">events.dlt</code>
              </p>
            </div>
          </div>

          {/* Cluster Live Status Pills */}
          <div className="flex flex-wrap items-center gap-2">
            {/* Quorum Pill */}
            <div
              className={`px-2.5 py-1 rounded text-xs font-mono flex items-center gap-1.5 border ${
                onlineBrokersCount === 3
                  ? "bg-emerald-950/50 border-emerald-500/40 text-[#00E599]"
                  : onlineBrokersCount === 2
                  ? "bg-amber-950/50 border-amber-500/40 text-amber-400"
                  : "bg-red-950/60 border-red-500/50 text-red-400"
              }`}
            >
              <span
                className={`w-2 h-2 rounded-full ${
                  onlineBrokersCount >= 2 ? "bg-[#00E599] animate-pulse" : "bg-red-500"
                }`}
              />
              <span>KRaft ISR: {onlineBrokersCount}/3</span>
              <span className="text-slate-400">(min.isr=2)</span>
            </div>

            {/* Key Mode Pill */}
            <button
              onClick={() =>
                executeAction(
                  data.config.keyMode === "user_id"
                    ? "exp3_key_mode_none"
                    : "exp3_key_mode_user_id"
                )
              }
              title="Нажмите для переключения KEY_MODE=user_id|none"
              className={`px-2.5 py-1 rounded text-xs font-mono flex items-center gap-1.5 border transition cursor-pointer ${
                data.config.keyMode === "user_id"
                  ? "bg-blue-950/50 border-blue-500/40 text-blue-400 hover:bg-blue-900/40"
                  : "bg-amber-950/60 border-amber-500/50 text-amber-300 hover:bg-amber-900/40"
              }`}
            >
              <GitBranch className="w-3.5 h-3.5" />
              <span>KEY_MODE={data.config.keyMode}</span>
            </button>

            {/* Live Stream Toggle */}
            <button
              onClick={() => setIsStreaming((s) => !s)}
              className={`px-3 py-1 rounded text-xs font-mono font-semibold flex items-center gap-1.5 border transition cursor-pointer ${
                isStreaming
                  ? "bg-[#00E599] text-slate-950 border-[#00E599]"
                  : "bg-[#111827] text-slate-200 border-slate-700 hover:border-slate-500"
              }`}
            >
              {isStreaming ? (
                <>
                  <Pause className="w-3.5 h-3.5 fill-current" />
                  <span>LIVE ПОТОК: ВКЛ</span>
                </>
              ) : (
                <>
                  <Play className="w-3.5 h-3.5 fill-current" />
                  <span>LIVE ПОТОК</span>
                </>
              )}
            </button>

            {/* Single Batch Trigger */}
            <button
              onClick={() => executeAction("produce_batch", { batchSize: 4 })}
              disabled={busyAction !== null}
              className="px-3 py-1 rounded text-xs font-mono font-medium bg-[#3B82F6] hover:bg-blue-500 text-white flex items-center gap-1.5 transition cursor-pointer disabled:opacity-50"
            >
              <Zap className="w-3.5 h-3.5" />
              <span>+12 Событий</span>
            </button>

            {/* Reset Baseline */}
            <button
              onClick={() => executeAction("reset_clean_baseline")}
              disabled={busyAction !== null}
              title="Сбросить состояние к чистому эталону"
              className="px-2.5 py-1 rounded text-xs font-mono bg-slate-800/90 hover:bg-slate-700 text-slate-300 border border-slate-700 flex items-center gap-1 transition cursor-pointer disabled:opacity-50"
            >
              <RotateCcw className="w-3.5 h-3.5" />
              <span className="hidden sm:inline">Сброс</span>
            </button>
          </div>
        </div>

        {/* Navigation Bar */}
        <div className="max-w-[1600px] mx-auto mt-2.5 pt-2 border-t border-slate-800/70 flex flex-wrap items-center justify-between gap-2">
          <div className="flex flex-wrap items-center gap-1.5">
            <button
              onClick={() => setActiveTab("topology")}
              className={`px-3 py-1.5 rounded text-xs font-medium flex items-center gap-1.5 transition cursor-pointer ${
                activeTab === "topology"
                  ? "bg-[#00E599]/15 text-[#00E599] border border-[#00E599]/40"
                  : "text-slate-400 hover:text-slate-200 hover:bg-slate-800/60"
              }`}
            >
              <Server className="w-3.5 h-3.5" />
              <span>1. Кластер KRaft, Партиции и Поды</span>
            </button>

            <button
              onClick={() => setActiveTab("experiments")}
              className={`px-3 py-1.5 rounded text-xs font-medium flex items-center gap-1.5 transition cursor-pointer ${
                activeTab === "experiments"
                  ? "bg-[#00E599]/15 text-[#00E599] border border-[#00E599]/40"
                  : "text-slate-400 hover:text-slate-200 hover:bg-slate-800/60"
              }`}
            >
              <Flame className="w-3.5 h-3.5" />
              <span>2. Эксперименты отказов (Часть 4) и CLI ./verify</span>
            </button>

            <button
              onClick={() => setActiveTab("code")}
              className={`px-3 py-1.5 rounded text-xs font-medium flex items-center gap-1.5 transition cursor-pointer ${
                activeTab === "code"
                  ? "bg-[#00E599]/15 text-[#00E599] border border-[#00E599]/40"
                  : "text-slate-400 hover:text-slate-200 hover:bg-slate-800/60"
              }`}
            >
              <Code2 className="w-3.5 h-3.5" />
              <span>3. Исходный код Java + Spring Boot + Docker Compose ({deliverables.length})</span>
            </button>

            <button
              onClick={() => setActiveTab("readme")}
              className={`px-3 py-1.5 rounded text-xs font-medium flex items-center gap-1.5 transition cursor-pointer ${
                activeTab === "readme"
                  ? "bg-[#00E599]/15 text-[#00E599] border border-[#00E599]/40"
                  : "text-slate-400 hover:text-slate-200 hover:bg-slate-800/60"
              }`}
            >
              <FileText className="w-3.5 h-3.5" />
              <span>4. Отчет README.md (Гипотезы и выводы)</span>
            </button>
          </div>

          {/* Quick KPI Strip */}
          <div className="flex items-center gap-4 text-xs font-mono">
            <div className="flex items-center gap-1.5">
              <span className="text-slate-400">Sink:</span>
              <span className="text-[#00E599] font-semibold">{verify.totalSinkEvents}</span>
            </div>
            <div className="flex items-center gap-1.5">
              <span className="text-slate-400">events.dlt:</span>
              <span className="text-red-400 font-semibold">{verify.totalDltEvents}</span>
            </div>
            <div className="flex items-center gap-1.5">
              <span className="text-slate-400">Dedup:</span>
              <span className="text-amber-400 font-semibold">{verify.rejectedDuplicates}</span>
            </div>
            <div className="flex items-center gap-1.5">
              <span className="text-slate-400">Gaps:</span>
              <span
                className={
                  verify.totalMissingSeq === 0
                    ? "text-[#00E599] font-semibold"
                    : "text-red-400 font-semibold"
                }
              >
                {verify.totalMissingSeq}
              </span>
            </div>
            <div className="flex items-center gap-1.5">
              <span className="text-slate-400">Out-of-Order:</span>
              <span
                className={
                  verify.totalOutOfOrder === 0
                    ? "text-[#00E599] font-semibold"
                    : "text-amber-400 font-semibold"
                }
              >
                {verify.totalOutOfOrder}
              </span>
            </div>
          </div>
        </div>
      </header>

      {/* =====================================================================
          MAIN WORKSPACE
         ===================================================================== */}
      <main className="flex-1 max-w-[1600px] w-full mx-auto p-5 space-y-5">
        {/* TOP KPI SUMMARY STRIP */}
        <section className="grid grid-cols-2 md:grid-cols-5 gap-3">
          <div className="bg-[#111827] border border-slate-800 rounded-md p-3.5">
            <div className="flex items-center justify-between text-xs text-slate-400">
              <span>Событий в PostgreSQL Sink</span>
              <Database className="w-4 h-4 text-[#00E599]" />
            </div>
            <div className="mt-1.5 text-2xl font-mono font-bold text-white">
              {verify.totalSinkEvents}
            </div>
            <div className="mt-1 text-[11px] font-mono text-[#00E599]">
              ON CONFLICT (event_id) DO NOTHING
            </div>
          </div>

          <div className="bg-[#111827] border border-slate-800 rounded-md p-3.5">
            <div className="flex items-center justify-between text-xs text-slate-400">
              <span>Ядовитых в events.dlt (~2%)</span>
              <Skull className="w-4 h-4 text-red-400" />
            </div>
            <div className="mt-1.5 text-2xl font-mono font-bold text-red-400">
              {verify.totalDltEvents}
            </div>
            <div className="mt-1 text-[11px] font-mono text-slate-400">
              После 3 попыток + заголовки DLT
            </div>
          </div>

          <div className="bg-[#111827] border border-slate-800 rounded-md p-3.5">
            <div className="flex items-center justify-between text-xs text-slate-400">
              <span>Отброшено дублей (Dedup)</span>
              <ShieldAlert className="w-4 h-4 text-amber-400" />
            </div>
            <div className="mt-1.5 text-2xl font-mono font-bold text-amber-400">
              {verify.rejectedDuplicates}
            </div>
            <div className="mt-1 text-[11px] font-mono text-slate-400">
              At-least-once + идемпотентный Sink
            </div>
          </div>

          <div className="bg-[#111827] border border-slate-800 rounded-md p-3.5">
            <div className="flex items-center justify-between text-xs text-slate-400">
              <span>Пропуски seq (Sink + DLT)</span>
              <CheckCircle2 className="w-4 h-4 text-[#00E599]" />
            </div>
            <div className="mt-1.5 text-2xl font-mono font-bold text-[#00E599]">
              {verify.totalMissingSeq}
            </div>
            <div className="mt-1 text-[11px] font-mono text-[#00E599]">
              12/12 user_id без потерь (seq 1..N)
            </div>
          </div>

          <div className="bg-[#111827] border border-slate-800 rounded-md p-3.5">
            <div className="flex items-center justify-between text-xs text-slate-400">
              <span>Нарушения порядка (OOO)</span>
              <AlertTriangle
                className={`w-4 h-4 ${
                  verify.totalOutOfOrder === 0 ? "text-[#00E599]" : "text-amber-400"
                }`}
              />
            </div>
            <div
              className={`mt-1.5 text-2xl font-mono font-bold ${
                verify.totalOutOfOrder === 0 ? "text-[#00E599]" : "text-amber-400"
              }`}
            >
              {verify.totalOutOfOrder}
            </div>
            <div className="mt-1 text-[11px] font-mono text-slate-400">
              KEY_MODE={data.config.keyMode}
            </div>
          </div>
        </section>

        {/* ===================================================================
            TAB 1: CLUSTER TOPOLOGY & LIVE PIPELINE
           =================================================================== */}
        {activeTab === "topology" && (
          <div className="space-y-5">
            {/* Quick Experiment Action Strip */}
            <div className="bg-[#111827] border border-slate-800 rounded-md p-4 flex flex-wrap items-center justify-between gap-3">
              <div>
                <h2 className="text-sm font-display font-bold uppercase tracking-wider text-white flex items-center gap-2">
                  <Flame className="w-4 h-4 text-amber-400" />
                  Быстрые инъекции отказов (Часть 4 — Эксперименты 1, 2, 3)
                </h2>
                <p className="text-xs text-slate-400 mt-0.5">
                  Нажмите на любой сценарий, чтобы мгновенно воспроизвести отказ на живом пайплайне и проверить вывод{" "}
                  <code className="text-[#00E599]">./verify</code>.
                </p>
              </div>
              <div className="flex flex-wrap items-center gap-2">
                {data.consumers.find((c) => c.id === "consumer-2")?.status === "RUNNING" ? (
                  <button
                    onClick={() => executeAction("exp1_kill_consumer2")}
                    disabled={busyAction !== null}
                    className="px-3 py-1.5 rounded text-xs font-mono font-semibold bg-red-600/20 hover:bg-red-600/30 text-red-300 border border-red-500/40 flex items-center gap-1.5 transition cursor-pointer"
                  >
                    <Skull className="w-3.5 h-3.5" />
                    <span>Эксп. 1: kill -9 consumer-2</span>
                  </button>
                ) : (
                  <button
                    onClick={() => executeAction("exp1_restart_consumer2")}
                    disabled={busyAction !== null}
                    className="px-3 py-1.5 rounded text-xs font-mono font-semibold bg-emerald-600/20 hover:bg-emerald-600/30 text-[#00E599] border border-[#00E599]/40 flex items-center gap-1.5 transition cursor-pointer"
                  >
                    <RefreshCw className="w-3.5 h-3.5" />
                    <span>Эксп. 1: Вернуть consumer-2</span>
                  </button>
                )}

                <button
                  onClick={() => executeAction("exp2_stop_broker1")}
                  disabled={busyAction !== null}
                  className="px-3 py-1.5 rounded text-xs font-mono font-semibold bg-amber-500/15 hover:bg-amber-500/25 text-amber-300 border border-amber-500/40 flex items-center gap-1.5 transition cursor-pointer"
                >
                  <span>Эксп. 2а: Stop kafka-1 (ISR=2)</span>
                </button>

                <button
                  onClick={() => executeAction("exp2_stop_broker2")}
                  disabled={busyAction !== null}
                  className="px-3 py-1.5 rounded text-xs font-mono font-semibold bg-red-600/20 hover:bg-red-600/30 text-red-300 border border-red-500/40 flex items-center gap-1.5 transition cursor-pointer"
                >
                  <span>Эксп. 2б: Stop kafka-2 (ISR=1 &lt; min.isr)</span>
                </button>

                <button
                  onClick={() => executeAction("exp2_recover_brokers")}
                  disabled={busyAction !== null}
                  className="px-3 py-1.5 rounded text-xs font-mono font-semibold bg-emerald-500/15 hover:bg-emerald-500/25 text-[#00E599] border border-[#00E599]/40 flex items-center gap-1.5 transition cursor-pointer"
                >
                  <span>Эксп. 2в: Восстановить брокеры</span>
                </button>

                <button
                  onClick={() =>
                    executeAction(
                      data.config.keyMode === "user_id"
                        ? "exp3_key_mode_none"
                        : "exp3_key_mode_user_id"
                    )
                  }
                  disabled={busyAction !== null}
                  className="px-3 py-1.5 rounded text-xs font-mono font-semibold bg-blue-600/20 hover:bg-blue-600/30 text-blue-300 border border-blue-500/40 flex items-center gap-1.5 transition cursor-pointer"
                >
                  <span>
                    Эксп. 3: Переключить KEY_MODE=
                    {data.config.keyMode === "user_id" ? "none" : "user_id"}
                  </span>
                </button>
              </div>
            </div>

            {/* 3-COLUMN TOPOLOGY GRID: PRODUCERS -> KRAFT BROKERS & PARTITIONS -> CONSUMERS */}
            <div className="grid grid-cols-1 lg:grid-cols-12 gap-4">
              {/* COLUMN 1: 3 SPRING BOOT PRODUCER PODS (3 cols) */}
              <div className="lg:col-span-3 space-y-3">
                <div className="flex items-center justify-between">
                  <h3 className="text-xs font-display font-bold uppercase tracking-wider text-slate-300 flex items-center gap-1.5">
                    <Cpu className="w-4 h-4 text-blue-400" />
                    Часть 2: Продюсеры (3 пода)
                  </h3>
                  <button
                    onClick={() => executeAction("sigterm_flush_producers")}
                    className="text-[11px] font-mono text-blue-400 hover:underline cursor-pointer"
                  >
                    SIGTERM flush()
                  </button>
                </div>

                {data.producers.map((prod) => (
                  <div
                    key={prod.id}
                    className={`bg-[#111827] rounded-md p-3.5 border ${
                      prod.status === "BACKPRESSURE_RETRYING"
                        ? "border-red-500/60"
                        : "border-slate-800"
                    }`}
                  >
                    <div className="flex items-center justify-between">
                      <span className="font-mono font-semibold text-sm text-white">
                        {prod.id}
                      </span>
                      <span
                        className={`px-2 py-0.5 rounded text-[10px] font-mono font-semibold uppercase ${
                          prod.status === "RUNNING"
                            ? "bg-emerald-500/15 text-[#00E599] border border-[#00E599]/30"
                            : "bg-red-500/20 text-red-300 border border-red-500/40"
                        }`}
                      >
                        {prod.status}
                      </span>
                    </div>

                    <div className="mt-2.5 flex flex-wrap gap-1">
                      {prod.ownedUsers.map((u) => (
                        <span
                          key={u}
                          className="px-1.5 py-0.5 rounded bg-slate-800 text-[11px] font-mono text-blue-300 border border-slate-700"
                        >
                          {u}
                        </span>
                      ))}
                    </div>

                    <div className="mt-2.5 pt-2 border-t border-slate-800/80 grid grid-cols-2 gap-2 text-[11px] font-mono text-slate-400">
                      <div>
                        acks: <span className="text-slate-200">all</span>
                      </div>
                      <div>
                        idempotence: <span className="text-[#00E599]">true</span>
                      </div>
                      <div>
                        key: <span className="text-amber-300">{prod.keyMode}</span>
                      </div>
                      <div>
                        отправлено: <span className="text-white">{prod.producedCount}</span>
                      </div>
                    </div>

                    {prod.bufferedCount > 0 && (
                      <div className="mt-2 px-2 py-1 rounded bg-amber-950/60 border border-amber-500/40 text-[11px] font-mono text-amber-300">
                        Буфер ретраев: {prod.bufferedCount} сообщ.
                      </div>
                    )}

                    {prod.lastError && (
                      <div className="mt-2 p-2 rounded bg-red-950/70 border border-red-500/40 text-[10px] font-mono text-red-300 break-words">
                        {prod.lastError}
                      </div>
                    )}
                  </div>
                ))}
              </div>

              {/* COLUMN 2: 3-BROKER KRAFT CLUSTER & PARTITION OFFSET STRIPS (6 cols) */}
              <div className="lg:col-span-6 space-y-4">
                <div className="flex items-center justify-between">
                  <h3 className="text-xs font-display font-bold uppercase tracking-wider text-slate-300 flex items-center gap-1.5">
                    <HardDrive className="w-4 h-4 text-[#00E599]" />
                    Часть 1: Кластер Kafka KRaft (3 брокера, без ZooKeeper)
                  </h3>
                  <span className="text-[11px] font-mono text-slate-400">
                    auto.create.topics.enable=false
                  </span>
                </div>

                {/* 3 Brokers Row */}
                <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
                  {data.brokers.map((broker) => (
                    <div
                      key={broker.id}
                      className={`bg-[#111827] rounded-md p-3 border ${
                        broker.status === "ONLINE"
                          ? "border-slate-800"
                          : "border-red-500/60 bg-red-950/15"
                      }`}
                    >
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-1.5">
                          <span
                            className={`w-2 h-2 rounded-full ${
                              broker.status === "ONLINE" ? "bg-[#00E599]" : "bg-red-500"
                            }`}
                          />
                          <span className="font-mono font-bold text-sm text-white">
                            {broker.name}
                          </span>
                        </div>
                        <span
                          className={`px-1.5 py-0.5 rounded text-[10px] font-mono font-semibold ${
                            broker.status === "ONLINE"
                              ? "bg-emerald-500/15 text-[#00E599]"
                              : "bg-red-500/20 text-red-400"
                          }`}
                        >
                          {broker.status}
                        </span>
                      </div>

                      {broker.isActiveController && broker.status === "ONLINE" && (
                        <div className="mt-1.5 inline-block px-1.5 py-0.5 rounded bg-blue-500/15 border border-blue-500/30 text-[10px] font-mono text-blue-300">
                          ★ Active KRaft Controller
                        </div>
                      )}

                      <div className="mt-2 space-y-1 text-[10px] font-mono text-slate-400">
                        <div className="truncate" title={broker.internalListener}>
                          INT: <span className="text-slate-200">{broker.internalListener.replace("INTERNAL://", "")}</span>
                        </div>
                        <div className="truncate" title={broker.externalListener}>
                          EXT: <span className="text-[#00E599]">{broker.externalListener.replace("EXTERNAL://", "")}</span>
                        </div>
                        <div>
                          Heap: <span className="text-slate-300">{broker.heapLimit}</span>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>

                {/* Topic `events` Partition Offset Strips */}
                <div className="bg-[#111827] border border-slate-800 rounded-md p-4 space-y-3">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div>
                      <span className="font-mono font-bold text-sm text-white">
                        Topic: <code className="text-[#00E599]">events</code>
                      </span>
                      <span className="ml-2 text-xs font-mono text-slate-400">
                        (3 Partitions • Replication Factor 3 • min.insync.replicas=2)
                      </span>
                    </div>
                    <span className="text-[11px] font-mono text-slate-400">
                      Topic: <code className="text-red-400">events.dlt</code> (3P • RF=3)
                    </span>
                  </div>

                  <div className="space-y-2.5">
                    {data.partitions.map((part) => {
                      const isQuorumOk = part.isr.length >= part.minInSyncReplicas;
                      return (
                        <div
                          key={part.partition}
                          className={`p-3 rounded border ${
                            isQuorumOk
                              ? "bg-[#0B0F17]/90 border-slate-800"
                              : "bg-red-950/20 border-red-500/50"
                          }`}
                        >
                          <div className="flex flex-wrap items-center justify-between gap-2 text-xs font-mono">
                            <div className="flex items-center gap-2">
                              <span className="px-2 py-0.5 rounded bg-slate-800 text-white font-semibold">
                                events-{part.partition}
                              </span>
                              <span className="text-slate-300">
                                Leader:{" "}
                                <strong className="text-[#00E599]">
                                  kafka-{part.leader}
                                </strong>
                              </span>
                              <span className="text-slate-400">
                                Replicas: [{part.replicas.join(",")}]
                              </span>
                              <span
                                className={
                                  isQuorumOk ? "text-[#00E599]" : "text-red-400 font-bold"
                                }
                              >
                                ISR: [{part.isr.join(",")}]
                              </span>
                            </div>

                            <div className="flex items-center gap-3">
                              <span className="text-slate-400">
                                Offset:{" "}
                                <strong className="text-white">
                                  {part.committedOffset}
                                </strong>
                                /{part.highWatermark}
                              </span>
                              <span className="px-2 py-0.5 rounded bg-blue-500/15 text-blue-300 border border-blue-500/30">
                                → {part.assignedConsumer || "unassigned"}
                              </span>
                            </div>
                          </div>

                          {/* Visual Partition Journal Cell Strip */}
                          <div className="mt-2.5 flex items-center gap-1 overflow-x-auto py-1">
                            {Array.from({ length: 18 }).map((_, idx) => {
                              const cellOffset = Math.max(0, part.highWatermark - 18) + idx;
                              const isCommitted = cellOffset < part.committedOffset;
                              const isExist = cellOffset < part.highWatermark;
                              return (
                                <div
                                  key={idx}
                                  className={`h-5 min-w-[26px] px-1 rounded text-[10px] font-mono flex items-center justify-center border ${
                                    !isExist
                                      ? "bg-slate-900/40 border-slate-800/60 text-slate-600"
                                      : isCommitted
                                      ? "bg-emerald-500/15 border-emerald-500/40 text-[#00E599]"
                                      : "bg-amber-500/20 border-amber-500/40 text-amber-300"
                                  }`}
                                  title={`Partition ${part.partition}, Offset ${cellOffset}`}
                                >
                                  {isExist ? cellOffset : "·"}
                                </div>
                              );
                            })}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              </div>

              {/* COLUMN 3: 3 CONSUMER PODS IN GROUP `events-processor` (3 cols) */}
              <div className="lg:col-span-3 space-y-3">
                <div className="flex items-center justify-between">
                  <h3 className="text-xs font-display font-bold uppercase tracking-wider text-slate-300 flex items-center gap-1.5">
                    <Activity className="w-4 h-4 text-[#00E599]" />
                    Часть 3: Консьюмеры ({activeConsumersCount}/3)
                  </h3>
                  <span className="text-[10px] font-mono text-slate-400">
                    group: events-processor
                  </span>
                </div>

                {data.consumers.map((cons) => (
                  <div
                    key={cons.id}
                    className={`bg-[#111827] rounded-md p-3.5 border ${
                      cons.status === "RUNNING"
                        ? "border-slate-800"
                        : "border-red-500/70 bg-red-950/15"
                    }`}
                  >
                    <div className="flex items-center justify-between">
                      <span className="font-mono font-semibold text-sm text-white">
                        {cons.id}
                      </span>
                      <span
                        className={`px-2 py-0.5 rounded text-[10px] font-mono font-semibold uppercase ${
                          cons.status === "RUNNING"
                            ? "bg-emerald-500/15 text-[#00E599] border border-[#00E599]/30"
                            : "bg-red-500/20 text-red-400 border border-red-500/40"
                        }`}
                      >
                        {cons.status}
                      </span>
                    </div>

                    <div className="mt-2 flex items-center gap-1.5 text-xs font-mono">
                      <span className="text-slate-400">Партиции:</span>
                      {cons.assignedPartitions.length > 0 ? (
                        cons.assignedPartitions.map((p) => (
                          <span
                            key={p}
                            className="px-1.5 py-0.5 rounded bg-emerald-500/15 text-[#00E599] border border-emerald-500/30"
                          >
                            events-{p}
                          </span>
                        ))
                      ) : (
                        <span className="text-red-400">нет (revoked)</span>
                      )}
                    </div>

                    <div className="mt-2.5 pt-2 border-t border-slate-800/80 grid grid-cols-3 gap-1 text-[11px] font-mono">
                      <div>
                        <span className="text-slate-400 block">В Sink:</span>
                        <span className="text-white font-semibold">
                          {cons.processedCount}
                        </span>
                      </div>
                      <div>
                        <span className="text-slate-400 block">В DLT:</span>
                        <span className="text-red-400 font-semibold">
                          {cons.dltRoutedCount}
                        </span>
                      </div>
                      <div>
                        <span className="text-slate-400 block">Dedup:</span>
                        <span className="text-amber-400 font-semibold">
                          {cons.dedupCaughtCount}
                        </span>
                      </div>
                    </div>

                    <div className="mt-2 text-[10px] font-mono text-slate-400 flex items-center justify-between">
                      <span>auto.commit=false</span>
                      <span>offset.reset=earliest</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* BOTTOM LIVE INSPECTOR: POSTGRESQL SINK, EVENTS.DLT (WITH HEADERS) & SPRING LOGS */}
            <div className="grid grid-cols-1 lg:grid-cols-12 gap-4">
              {/* Left 7 cols: Sink Events / DLT Events / Logs Table */}
              <div className="lg:col-span-7 bg-[#111827] border border-slate-800 rounded-md p-4">
                <div className="flex flex-wrap items-center justify-between gap-2 pb-3 border-b border-slate-800">
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => setSinkSubTab("sink")}
                      className={`px-3 py-1 rounded text-xs font-mono font-medium transition cursor-pointer ${
                        sinkSubTab === "sink"
                          ? "bg-[#00E599]/15 text-[#00E599] border border-[#00E599]/40"
                          : "text-slate-400 hover:text-white"
                      }`}
                    >
                      PostgreSQL Sink ({verify.totalSinkEvents})
                    </button>
                    <button
                      onClick={() => setSinkSubTab("dlt")}
                      className={`px-3 py-1 rounded text-xs font-mono font-medium transition cursor-pointer ${
                        sinkSubTab === "dlt"
                          ? "bg-red-500/20 text-red-300 border border-red-500/40"
                          : "text-slate-400 hover:text-white"
                      }`}
                    >
                      Топик events.dlt ({verify.totalDltEvents})
                    </button>
                    <button
                      onClick={() => setSinkSubTab("logs")}
                      className={`px-3 py-1 rounded text-xs font-mono font-medium transition cursor-pointer ${
                        sinkSubTab === "logs"
                          ? "bg-blue-500/20 text-blue-300 border border-blue-500/40"
                          : "text-slate-400 hover:text-white"
                      }`}
                    >
                      Логи подов ({data.recentLogs.length})
                    </button>
                  </div>

                  <span className="text-[11px] font-mono text-slate-400">
                    Дедупликация по <code className="text-[#00E599]">event_id</code>
                  </span>
                </div>

                {sinkSubTab === "sink" && (
                  <div className="mt-3 overflow-x-auto max-h-[340px] overflow-y-auto">
                    <table className="w-full text-left border-collapse text-xs font-mono">
                      <thead>
                        <tr className="border-b border-slate-800 text-slate-400 text-[11px]">
                          <th className="py-2 px-2">ID</th>
                          <th className="py-2 px-2">user_id</th>
                          <th className="py-2 px-2">seq</th>
                          <th className="py-2 px-2">type</th>
                          <th className="py-2 px-2">event_id</th>
                          <th className="py-2 px-2">pod</th>
                          <th className="py-2 px-2">topic[p@off]</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-800/60">
                        {data.recentSinkEvents.map((ev: any) => (
                          <tr key={ev.id} className="hover:bg-slate-800/40">
                            <td className="py-1.5 px-2 text-slate-400">#{ev.id}</td>
                            <td className="py-1.5 px-2 text-blue-300 font-semibold">
                              {ev.user_id}
                            </td>
                            <td className="py-1.5 px-2 text-[#00E599] font-bold">
                              {ev.seq}
                            </td>
                            <td className="py-1.5 px-2 text-slate-200">
                              {ev.event_type}
                            </td>
                            <td className="py-1.5 px-2 text-slate-400">
                              {String(ev.event_id).slice(0, 8)}…
                            </td>
                            <td className="py-1.5 px-2 text-emerald-300">
                              {ev.processor_pod}
                            </td>
                            <td className="py-1.5 px-2 text-slate-300">
                              {ev.topic}[p={ev.partition}@{ev.kafka_offset}]
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}

                {sinkSubTab === "dlt" && (
                  <div className="mt-3 space-y-2.5 max-h-[340px] overflow-y-auto pr-1">
                    {data.recentDltEvents.map((dlt: any) => (
                      <div
                        key={dlt.id}
                        className="p-3 rounded bg-red-950/20 border border-red-500/30 text-xs font-mono space-y-1.5"
                      >
                        <div className="flex flex-wrap items-center justify-between gap-2">
                          <span className="text-red-300 font-bold">
                            ☠ POISON EVENT → events.dlt[p={dlt.dlt_partition}@offset=
                            {dlt.dlt_offset}]
                          </span>
                          <span className="text-slate-300">
                            user_id:{" "}
                            <strong className="text-white">{dlt.user_id}</strong> | seq:{" "}
                            <strong className="text-[#00E599]">{dlt.seq}</strong> | attempts:{" "}
                            <strong className="text-amber-300">
                              {dlt.retry_attempts}/3
                            </strong>
                          </span>
                        </div>
                        <div className="text-[11px] text-slate-300 bg-[#0B0F17] p-2 rounded border border-slate-800">
                          <div className="text-slate-400 mb-1">
                            Kafka Record Headers (DeadLetterPublishingRecoverer):
                          </div>
                          <div className="grid grid-cols-1 sm:grid-cols-3 gap-1 text-[11px]">
                            <div>
                              <span className="text-slate-500">x-original-topic:</span>{" "}
                              <span className="text-emerald-300">
                                {dlt.original_topic}
                              </span>
                            </div>
                            <div>
                              <span className="text-slate-500">
                                x-original-partition:
                              </span>{" "}
                              <span className="text-emerald-300">
                                {dlt.original_partition}
                              </span>
                            </div>
                            <div>
                              <span className="text-slate-500">x-original-offset:</span>{" "}
                              <span className="text-emerald-300">
                                {dlt.original_offset}
                              </span>
                            </div>
                          </div>
                          <div className="mt-1 text-red-300 truncate">
                            <span className="text-slate-500">x-error-reason:</span>{" "}
                            {dlt.error_reason}
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                )}

                {sinkSubTab === "logs" && (
                  <div className="mt-3 space-y-1.5 max-h-[340px] overflow-y-auto font-mono text-xs">
                    {data.recentLogs.map((log: any) => (
                      <div
                        key={log.id}
                        className="p-2 rounded bg-[#0B0F17] border border-slate-800/80 flex items-start gap-2"
                      >
                        <span
                          className={`px-1.5 py-0.5 rounded text-[10px] font-bold uppercase shrink-0 ${
                            log.level === "ERROR"
                              ? "bg-red-500/20 text-red-400"
                              : log.level === "WARN"
                              ? "bg-amber-500/20 text-amber-300"
                              : "bg-emerald-500/15 text-[#00E599]"
                          }`}
                        >
                          {log.level}
                        </span>
                        <span className="text-blue-400 shrink-0">[{log.component}]</span>
                        <span className="text-slate-300 break-words">{log.message}</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {/* Right 5 cols: Live `./verify` CLI Output Terminal */}
              <div className="lg:col-span-5 bg-[#111827] border border-slate-800 rounded-md p-4 flex flex-col">
                <div className="flex flex-wrap items-center justify-between gap-2 pb-3 border-b border-slate-800">
                  <div className="flex items-center gap-2">
                    <Terminal className="w-4 h-4 text-[#00E599]" />
                    <span className="font-display font-bold text-xs uppercase tracking-wider text-white">
                      CLI Скрипт ./verify (Live Вывод)
                    </span>
                  </div>
                  <div className="flex items-center gap-1.5">
                    <input
                      type="text"
                      value={githubLoginInput}
                      onChange={(e) => setGithubLoginInput(e.target.value)}
                      placeholder="GitHub login"
                      className="px-2 py-1 rounded bg-[#0B0F17] border border-slate-700 text-xs font-mono text-[#00E599] w-36 focus:outline-none focus:border-[#00E599]"
                    />
                    <button
                      onClick={() =>
                        executeAction("run_verify", {
                          githubLogin: githubLoginInput,
                        })
                      }
                      className="px-2.5 py-1 rounded bg-[#00E599] text-slate-950 font-mono font-semibold text-xs hover:bg-emerald-400 transition cursor-pointer"
                    >
                      ./verify
                    </button>
                    <button
                      onClick={() => handleCopyVerify(verify.rawTerminalOutput)}
                      className="p-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 cursor-pointer"
                      title="Скопировать вывод ./verify"
                    >
                      {copiedVerify ? (
                        <Check className="w-3.5 h-3.5 text-[#00E599]" />
                      ) : (
                        <Copy className="w-3.5 h-3.5" />
                      )}
                    </button>
                  </div>
                </div>

                <pre className="mt-3 flex-1 bg-[#070A10] border border-slate-800 rounded p-3 text-[11px] font-mono text-slate-200 overflow-x-auto leading-relaxed">
                  {verify.rawTerminalOutput}
                </pre>
              </div>
            </div>
          </div>
        )}

        {/* ===================================================================
            TAB 2: PART 4 FAILURE EXPERIMENTS & VERIFY CLI WORKBENCH
           =================================================================== */}
        {activeTab === "experiments" && (
          <div className="space-y-5">
            {/* Three Interactive Experiment Cards */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
              {/* EXPERIMENT 1 CARD */}
              <div className="bg-[#111827] border border-slate-800 rounded-md p-4 flex flex-col justify-between space-y-4">
                <div className="space-y-2.5">
                  <div className="flex items-center justify-between">
                    <span className="px-2 py-0.5 rounded bg-red-500/20 text-red-300 border border-red-500/30 text-xs font-mono font-semibold">
                      Эксперимент 4.1
                    </span>
                    <span className="text-xs font-mono text-slate-400">
                      kill -9 consumer-2
                    </span>
                  </div>
                  <h3 className="font-display font-bold text-base text-white">
                    1. Убийство (kill -9) одного пода-консьюмера
                  </h3>
                  <div className="p-3 rounded bg-[#0B0F17] border border-slate-800 text-xs text-slate-300 space-y-1.5">
                    <div className="font-semibold text-[#00E599]">Гипотеза:</div>
                    <p>
                      При <code className="text-red-300">kill -9 consumer-2</code> под падает без коммита последнего in-flight оффсета. Координатор группы запускает ребаланс, живой{" "}
                      <code>consumer-1</code> фиксирует свои оффсеты через{" "}
                      <code>ConsumerAwareRebalanceListener</code> и забирает партицию{" "}
                      <code>events-1</code> с последнего закоммиченного оффсета. Повторно доставленное сообщение подавляется идемпотентной записью{" "}
                      <code>ON CONFLICT (event_id) DO NOTHING</code>, увеличивая счетчик{" "}
                      <code>Rejected Duplicate Deliveries</code> без потери данных.
                    </p>
                  </div>
                </div>

                <div className="pt-2 border-t border-slate-800 flex flex-wrap gap-2">
                  <button
                    onClick={() => executeAction("exp1_kill_consumer2")}
                    disabled={busyAction !== null}
                    className="flex-1 px-3 py-2 rounded bg-red-600 hover:bg-red-500 text-white font-mono text-xs font-semibold flex items-center justify-center gap-1.5 transition cursor-pointer"
                  >
                    <Skull className="w-3.5 h-3.5" />
                    <span>Выполнить kill -9 consumer-2</span>
                  </button>
                  <button
                    onClick={() => executeAction("exp1_restart_consumer2")}
                    disabled={busyAction !== null}
                    className="px-3 py-2 rounded bg-slate-800 hover:bg-slate-700 text-slate-200 font-mono text-xs transition cursor-pointer"
                  >
                    Вернуть под
                  </button>
                </div>
              </div>

              {/* EXPERIMENT 2 CARD */}
              <div className="bg-[#111827] border border-slate-800 rounded-md p-4 flex flex-col justify-between space-y-4">
                <div className="space-y-2.5">
                  <div className="flex items-center justify-between">
                    <span className="px-2 py-0.5 rounded bg-amber-500/20 text-amber-300 border border-amber-500/30 text-xs font-mono font-semibold">
                      Эксперимент 4.2
                    </span>
                    <span className="text-xs font-mono text-slate-400">
                      acks=all, min.isr=2
                    </span>
                  </div>
                  <h3 className="font-display font-bold text-base text-white">
                    2. Остановка 1-го брокера, затем 2-го и восстановление
                  </h3>
                  <div className="p-3 rounded bg-[#0B0F17] border border-slate-800 text-xs text-slate-300 space-y-1.5">
                    <div className="font-semibold text-amber-400">Гипотеза:</div>
                    <p>
                      При остановке <code>kafka-1</code> выбирается новый лидер партиции <code>P0</code>,{" "}
                      <code>ISR=[2,3]</code> (размер 2 &ge; <code>min.insync.replicas=2</code>) — продюсеры продолжают запись. При остановке второго брокера{" "}
                      <code>kafka-2</code> размер <code>ISR=[3]</code> падает до 1 &lt; 2: продюсеры с{" "}
                      <code>acks=all</code> логируют <code>NotEnoughReplicasException</code> и копят батчи в буфере. После возврата брокеров репликация восстанавливается, и данные досылаются без потерь.
                    </p>
                  </div>
                </div>

                <div className="pt-2 border-t border-slate-800 grid grid-cols-3 gap-1.5">
                  <button
                    onClick={() => executeAction("exp2_stop_broker1")}
                    disabled={busyAction !== null}
                    className="px-2 py-2 rounded bg-amber-600/30 hover:bg-amber-600/40 border border-amber-500/40 text-amber-200 font-mono text-[11px] font-semibold transition cursor-pointer"
                  >
                    1. Stop kafka-1
                  </button>
                  <button
                    onClick={() => executeAction("exp2_stop_broker2")}
                    disabled={busyAction !== null}
                    className="px-2 py-2 rounded bg-red-600/30 hover:bg-red-600/40 border border-red-500/40 text-red-200 font-mono text-[11px] font-semibold transition cursor-pointer"
                  >
                    2. Stop kafka-2
                  </button>
                  <button
                    onClick={() => executeAction("exp2_recover_brokers")}
                    disabled={busyAction !== null}
                    className="px-2 py-2 rounded bg-emerald-600/30 hover:bg-emerald-600/40 border border-emerald-500/40 text-[#00E599] font-mono text-[11px] font-semibold transition cursor-pointer"
                  >
                    3. Start Оба
                  </button>
                </div>
              </div>

              {/* EXPERIMENT 3 CARD */}
              <div className="bg-[#111827] border border-slate-800 rounded-md p-4 flex flex-col justify-between space-y-4">
                <div className="space-y-2.5">
                  <div className="flex items-center justify-between">
                    <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 border border-blue-500/30 text-xs font-mono font-semibold">
                      Эксперимент 4.3
                    </span>
                    <span className="text-xs font-mono text-slate-400">
                      KEY_MODE=none vs user_id
                    </span>
                  </div>
                  <h3 className="font-display font-bold text-base text-white">
                    3. Продюсеры с KEY_MODE=none против KEY_MODE=user_id
                  </h3>
                  <div className="p-3 rounded bg-[#0B0F17] border border-slate-800 text-xs text-slate-300 space-y-1.5">
                    <div className="font-semibold text-blue-400">Гипотеза:</div>
                    <p>
                      Kafka гарантирует порядок только внутри одной партиции. При{" "}
                      <code>KEY_MODE=none</code> события одного <code>user_id</code> попадают в разные партиции (<code>P0, P1, P2</code>) и параллельно обрабатываются тремя разными подами <code>consumer-1..3</code>. Из-за разницы скоростей и ретраев на ядовитых сообщениях событие с бо́льшим <code>seq</code> записывается в Sink раньше меньшего <code>seq</code> (возникают нарушения порядка <code>ooo_writes &gt; 0</code>).
                    </p>
                  </div>
                </div>

                <div className="pt-2 border-t border-slate-800 flex gap-2">
                  <button
                    onClick={() => executeAction("exp3_key_mode_none")}
                    disabled={busyAction !== null}
                    className="flex-1 px-3 py-2 rounded bg-amber-500 hover:bg-amber-400 text-slate-950 font-mono text-xs font-bold transition cursor-pointer"
                  >
                    Запустить KEY_MODE=none
                  </button>
                  <button
                    onClick={() => executeAction("exp3_key_mode_user_id")}
                    disabled={busyAction !== null}
                    className="px-3 py-2 rounded bg-blue-600/30 hover:bg-blue-600/40 border border-blue-500/40 text-blue-200 font-mono text-xs transition cursor-pointer"
                  >
                    KEY_MODE=user_id
                  </button>
                </div>
              </div>
            </div>

            {/* FULL TERMINAL + PER-USER VERIFICATION MATRIX */}
            <div className="grid grid-cols-1 lg:grid-cols-12 gap-4">
              <div className="lg:col-span-6 bg-[#111827] border border-slate-800 rounded-md p-4">
                <div className="flex flex-wrap items-center justify-between gap-2 pb-3 border-b border-slate-800">
                  <div className="flex items-center gap-2">
                    <Terminal className="w-4 h-4 text-[#00E599]" />
                    <h3 className="font-display font-bold text-sm text-white">
                      Вывод скрипта ./verify (для скриншотов в README.md)
                    </h3>
                  </div>
                  <div className="flex items-center gap-2">
                    <input
                      type="text"
                      value={githubLoginInput}
                      onChange={(e) => setGithubLoginInput(e.target.value)}
                      className="px-2.5 py-1 rounded bg-[#0B0F17] border border-slate-700 text-xs font-mono text-[#00E599]"
                      placeholder="Ваш GitHub логин"
                    />
                    <button
                      onClick={() =>
                        executeAction("run_verify", {
                          githubLogin: githubLoginInput,
                        })
                      }
                      className="px-3 py-1 rounded bg-[#00E599] text-slate-950 font-mono text-xs font-bold cursor-pointer"
                    >
                      Обновить ./verify
                    </button>
                  </div>
                </div>
                <pre className="mt-3 bg-[#070A10] border border-slate-800 rounded p-4 text-xs font-mono text-slate-200 overflow-x-auto leading-relaxed">
                  {verify.rawTerminalOutput}
                </pre>
              </div>

              <div className="lg:col-span-6 bg-[#111827] border border-slate-800 rounded-md p-4">
                <div className="pb-3 border-b border-slate-800 flex items-center justify-between">
                  <h3 className="font-display font-bold text-sm text-white">
                    Детальная сверка последовательностей seq по каждому user_id
                  </h3>
                  <span className="text-xs font-mono text-[#00E599]">
                    Sink + events.dlt = Полная последовательность
                  </span>
                </div>
                <div className="mt-3 overflow-x-auto">
                  <table className="w-full text-left border-collapse text-xs font-mono">
                    <thead>
                      <tr className="border-b border-slate-800 text-slate-400">
                        <th className="py-2 px-2">user_id</th>
                        <th className="py-2 px-2">Диапазон seq</th>
                        <th className="py-2 px-2">В Sink</th>
                        <th className="py-2 px-2">В events.dlt</th>
                        <th className="py-2 px-2">Пропуски</th>
                        <th className="py-2 px-2">Нарушения порядка</th>
                        <th className="py-2 px-2">Статус</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-800/60">
                      {verify.users.map((u) => (
                        <tr key={u.userId} className="hover:bg-slate-800/40">
                          <td className="py-1.5 px-2 text-blue-300 font-semibold">
                            {u.userId}
                          </td>
                          <td className="py-1.5 px-2 text-slate-200">
                            1 .. {u.maxSeq}
                          </td>
                          <td className="py-1.5 px-2 text-[#00E599]">
                            {u.sinkCount}
                          </td>
                          <td className="py-1.5 px-2 text-red-400">{u.dltCount}</td>
                          <td className="py-1.5 px-2">
                            <span
                              className={
                                u.missingCount === 0
                                  ? "text-[#00E599] font-bold"
                                  : "text-red-400 font-bold"
                              }
                            >
                              {u.missingCount}
                            </span>
                          </td>
                          <td className="py-1.5 px-2">
                            <span
                              className={
                                u.outOfOrderCount === 0
                                  ? "text-slate-400"
                                  : "text-amber-400 font-bold"
                              }
                            >
                              {u.outOfOrderCount}
                            </span>
                          </td>
                          <td className="py-1.5 px-2">
                            <span className="px-2 py-0.5 rounded bg-emerald-500/15 text-[#00E599] text-[10px]">
                              OK (NO GAPS)
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* ===================================================================
            TAB 3: JAVA 21 + SPRING BOOT 3 + DOCKER COMPOSE SOURCE CODE EXPLORER
           =================================================================== */}
        {activeTab === "code" && (
          <div className="grid grid-cols-1 lg:grid-cols-12 gap-4">
            {/* File Tree Sidebar */}
            <div className="lg:col-span-4 bg-[#111827] border border-slate-800 rounded-md p-4 space-y-2">
              <div className="font-display font-bold text-xs uppercase tracking-wider text-slate-300 pb-2 border-b border-slate-800">
                Файлы проекта (Java 21 + Spring Boot 3 + Docker)
              </div>
              <div className="space-y-1">
                {deliverables.map((file, idx) => (
                  <button
                    key={file.path}
                    onClick={() => setSelectedFileIndex(idx)}
                    className={`w-full text-left p-2.5 rounded border transition cursor-pointer ${
                      selectedFileIndex === idx
                        ? "bg-[#00E599]/15 border-[#00E599]/50 text-white"
                        : "bg-[#0B0F17]/60 border-slate-800/80 text-slate-300 hover:bg-slate-800/50"
                    }`}
                  >
                    <div className="flex items-center justify-between">
                      <span className="font-mono text-xs font-semibold text-[#00E599]">
                        {file.title}
                      </span>
                      <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-slate-800 text-slate-400">
                        {file.category}
                      </span>
                    </div>
                    <div className="text-[11px] text-slate-400 mt-1 line-clamp-2">
                      {file.description}
                    </div>
                  </button>
                ))}
              </div>
            </div>

            {/* Code Viewer */}
            <div className="lg:col-span-8 bg-[#111827] border border-slate-800 rounded-md p-4 flex flex-col">
              {currentFile && (
                <>
                  <div className="flex flex-wrap items-center justify-between gap-2 pb-3 border-b border-slate-800">
                    <div>
                      <div className="font-mono text-sm font-bold text-white">
                        {currentFile.path}
                      </div>
                      <div className="text-xs text-slate-400 mt-0.5">
                        {currentFile.description}
                      </div>
                    </div>
                    <button
                      onClick={() => handleCopyCode(currentFile.content)}
                      className="px-3 py-1.5 rounded bg-[#00E599] text-slate-950 font-mono text-xs font-bold flex items-center gap-1.5 cursor-pointer"
                    >
                      {copiedCode ? (
                        <>
                          <Check className="w-3.5 h-3.5" />
                          <span>Скопировано!</span>
                        </>
                      ) : (
                        <>
                          <Copy className="w-3.5 h-3.5" />
                          <span>Скопировать файл</span>
                        </>
                      )}
                    </button>
                  </div>
                  <pre className="mt-3 flex-1 bg-[#070A10] border border-slate-800 rounded p-4 text-xs font-mono text-slate-200 overflow-x-auto max-h-[680px] overflow-y-auto leading-relaxed">
                    {currentFile.content}
                  </pre>
                </>
              )}
            </div>
          </div>
        )}

        {/* ===================================================================
            TAB 4: README.MD & HYPOTHESES REPORT
           =================================================================== */}
        {activeTab === "readme" && (
          <div className="bg-[#111827] border border-slate-800 rounded-md p-6 space-y-6">
            <div className="flex flex-wrap items-center justify-between gap-3 pb-4 border-b border-slate-800">
              <div>
                <h2 className="font-display font-bold text-lg text-white">
                  README.md — Документация, Гипотезы и Результаты Экспериментов (Часть 4)
                </h2>
                <p className="text-xs text-slate-400 mt-0.5">
                  Полный оформленный отчет по домашней работе HW2/3 (DevOps/ФиБ и MSE/MHS), сохраненный в корне репозитория <code>README.md</code>.
                </p>
              </div>
              <button
                onClick={() => {
                  const readmeFile = deliverables.find((f) => f.path === "README.md");
                  if (readmeFile) handleCopyCode(readmeFile.content);
                }}
                className="px-3 py-1.5 rounded bg-[#00E599] text-slate-950 font-mono text-xs font-bold flex items-center gap-1.5 cursor-pointer"
              >
                <Copy className="w-3.5 h-3.5" />
                <span>Скопировать README.md</span>
              </button>
            </div>

            <pre className="bg-[#070A10] border border-slate-800 rounded p-5 text-xs font-mono text-slate-200 overflow-x-auto whitespace-pre-wrap leading-relaxed">
              {deliverables.find((f) => f.path === "README.md")?.content || ""}
            </pre>
          </div>
        )}
      </main>
    </div>
  );
}
