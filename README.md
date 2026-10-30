изменение для возможности открыть ПР

# Принципы построения высоконагруженных систем

## Материалы лекций

- [Лекция 1. Введение в высоконагруженные системы](lecture-01/L1.Introduction.pdf) (1 октября 2026)  
  Нагрузка, latency, throughput, RPS/QPS, concurrency, percentile-распределения, tail latency, SLA/SLO/SLI.

- [Лекция 2. Очереди и пределы производительности](lecture-02/L2.Queues.pdf) (8 октября 2026)  
  Закон Литтла, arrival/service rate, utilization, M/M/1, M/M/c, saturation, bursty traffic.

3. [Масштабирование](lecture-03/L3.Scaling.pdf) (15 октября 2026)  
   Закон Амдала, Universal Scalability Law, contention, vertical/horizontal scaling, bottleneck, capacity planning.

4. [Фундаментальные ограничения распределённых систем для highload](lecture-04/L4.Scaling.pdf) (22 октября 2026)  
   CAP, PACELC, репликация, consistency, quorum, consensus на обзорном уровне.

## Материалы семинаров, осень 2026.

- [Семинар 1. Мониторинг высоконагруженных систем](seminar-01/README.md)  (2 октября 2026)

## Начало работы

```bash
git clone https://github.com/itmo-ipkn/highload_autumn_2026.git
cd highload_autumn_2026/seminar-01
```

Первый семинар запускается локально с Docker Compose. Начните с `seminar-01/README.md`; команды запуска и экспериментов выполняются из `seminar-01/lab` на вашем компьютере.
