import type { Metadata } from "next";
import type { ReactNode } from "react";
import { Space_Grotesk, Inter, JetBrains_Mono } from "next/font/google";
import "./globals.css";

const spaceGrotesk = Space_Grotesk({
  subsets: ["latin", "latin-ext"],
  variable: "--font-display",
  weight: ["600", "700"],
});

const inter = Inter({
  subsets: ["latin", "cyrillic"],
  variable: "--font-sans",
  weight: ["400", "500", "600"],
});

const jetbrainsMono = JetBrains_Mono({
  subsets: ["latin", "cyrillic"],
  variable: "--font-mono",
  weight: ["400", "500", "600"],
});

export const metadata: Metadata = {
  title: "HW2/3. Очередь Kafka | Java 21 + Spring Boot 3 + KRaft Observatory",
  description:
    "DDIA Autumn 2026: 3-Broker Kafka KRaft Cluster, Idempotent Spring Boot Producers, At-Least-Once Consumers, Dead Letter Topic (events.dlt), PostgreSQL Sink Deduplication, and Failure Experiments.",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html
      lang="ru"
      className={`${spaceGrotesk.variable} ${inter.variable} ${jetbrainsMono.variable} dark`}
    >
      <body className="bg-[#0B0F17] text-[#F9FAFB] font-sans antialiased selection:bg-[#00E599]/20 selection:text-[#00E599]">
        {children}
      </body>
    </html>
  );
}
