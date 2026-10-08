# Demo talk track

2026-10-08

About 10 to 11 minutes over the six chapters of the demo page: opening and requirements 1, chapter 2 1.5, chapter 3 3, chapter 4 1.5, deep dives 2 to 3, chapter 6 1. Pick two or three deep dives by what the audience asks. The route stays hidden until chapter 4. Each section says what to point at and the lines to say aloud. This doc is for the speaker; the page is what the audience sees. 中文是写给讲者理解的，英文是要念出来的台词。

## Opening and chapter 1 · Requirements (1 min)

**要点：** 像系统设计面试一样，先讲需求。左边功能需求，右边非功能需求。只念重点，不讲状态；状态放在最后一章。

**Opening.** Point at the title, then the eyebrow line.

1. Today I show one option for account-grain revenue, not a decision.
2. The question is the title: can Airflow orchestrate 100,000 partitions?
3. Each test ran once, in a local environment, not MWAA. Spark is a stand-in.

**Requirements.** Point at the two lists, left then right.

1. Functionally: start by event, recompute only what changed, batch partitions into one Spark job, chain downstream.
2. Plus what operators need: isolate a bad partition, rerun on demand, and see per-partition status.
3. Non-functionally: 100,000 partitions, an adjustment p95 under two minutes, and no wrong result when work overlaps.
4. And it should use Airflow as designed, on MWAA 3.3.1.

## Chapter 2 · What an orchestrator does, and how Airflow does it (1.5 min)

**要点：** 只讲"编排器做哪几件事、原生 Airflow 怎么做"，不提我们、不提 batch、不提 100k 的代价。图上只有一个分区，也就是一个 Dag run。用 Next 逐个点亮七件工作，每一下说一句。点完问"有 100,000 个这样的 run 会怎样"，接第 3 章。

Do not say "we", "our", "batch" or "journal" in this part.

**Overview.** Point at the whole picture.

1. Whatever grain the calculation team picks, an orchestrator does the same seven jobs.
2. Here is one partition in native Airflow: one Dag run, with its tasks and events.
3. Watch where each job hangs on this run.

**Click Next for each job, one sentence each.**

1. Trigger: a keyed asset event wakes the Dag. One event and one run per partition.
2. Dependencies: the run emits an event that wakes the downstream run for the same key.
3. Dispatch: its task instances queue through pools and max_active_runs.
4. Retry: a failed task instance retries. One partition per run, so retries are per partition.
5. Status: the run state shows in the UI, and I can filter runs by key.
6. Rerun: I clear this run, or trigger one for this key.
7. Lineage: the run records the events it consumed.

**After job 7.** Point at the paragraph under the picture.

1. So per-partition retry, rerun and lineage come out of the box. That is the strength.
2. Now picture 100,000 of these runs. That is chapter 3.

## Chapter 3 · Why native breaks: two separate problems (3 min, the core)

**要点：** 这是核心。两个问题分开讲。问题一是编排：Airflow 的成本跟它跟踪的对象个数成正比，因为每个对象都是数据库行，调度器是一个 Python 循环，每个 run 至少看两遍，有些步骤一个事务碰所有行。问题二是计算：引擎跑多少个 job，跟问题一无关，换成 Lambda 也不改变问题一。然后用三格图证明：100k 个 run，26 分钟才完成 3,698 个；1 个 run 带 100k 个 task，卡死 317 秒；分批映射 38 秒能跑，但不知道什么变了。最后停在问题上：能不能既像分批那样少对象，又按分区知道什么变了、什么完成了？先别翻到第 4 章。

The only batching in this part is native batching, in the third picture and its "Inside" tab. Do not say "our route" yet.

**0:00–0:10 · Title and lead.** Point at the title and the lead under it.

1. Why is that a problem? I see two separate questions.
2. Can Airflow track 100,000 things? And how many engine jobs run?

**0:10–0:40 · Problem 1 card.** Point at the four bold lines, in order: every object is rows, one loop looks at them, some steps touch all of them at once, so the count is what breaks it.

1. Problem 1: every run, task and event is a row in Airflow's database.
2. The scheduler is one Python loop over those rows.
3. Each run needs at least two visits: start the task, then see it finish.
4. Some steps touch all rows at once: mapped tasks expand in one transaction.
5. So the object count is what breaks it.
6. Data size costs time too, but it does not freeze the scheduler.

**0:40–1:00 · Problem 2 card.** Point at the Problem 2 card. End on "whatever the engine, problem 1 stays the same."

1. Problem 2 is compute: how many engine jobs, and how big.
2. Spark pays start-up per job, so one job per partition pays it 100,000 times.
3. Lambda per partition might work, if the fan-out stays inside one Airflow task.
4. We have not measured any engine yet.
5. And whatever the engine, problem 1 stays the same.

**1:00–1:50 · Where the 100,000 go.** Point at the three pictures, left to right, then the note under them.

1. Back to problem 1, measured.
2. Left: 100,000 runs, one task each. Each square is 250 runs.
3. The loop was busy creating runs.
4. After 26 minutes, 3,698 runs were marked finished, though 80,900 tasks had run.
5. With extra indexes and two schedulers, all 100,000 took 31 minutes.
6. Middle: one run with 100,000 tasks, all created in one transaction.
7. The scheduler froze for 317 seconds. Its health limit is 30.
8. Right: one run with 100 tasks of 1,000 partitions. 101 rows, 38 seconds.
9. Batching keeps the count low, but that task was a no-op.
10. And it does not know what changed, or record what is done.
11. MWAA can add schedulers, but gives no database access for the indexes.

**Look inside, about 1 minute.** Click a picture, or the "Inside" tab under the pictures. Point at the numbered stages and the red table.

中文提示：这一段讲原理，按图上的 ①②③ 一步一句。方式一卡在"登记 key"和"收尾"，方式二卡在"一个事务里插 100,000 行"，方式三快但不知道哪些分区变了。

Inside 1, 100,000 runs:

1. One: the producer's one request registers every key. Each key is three inserts, plus a lookup on a queue table with no index.
2. That is about 5 ms per key. The request must finish in 5 seconds, so 500 keys at a time: 25 minutes.
3. Two: the scheduler turns each queued key into a run and a task, 500 per loop.
4. Three: each run needs two scheduler visits, and new runs go first. So finished runs wait.

Inside 2, 100,000 tasks:

1. The scheduler expands the task in one loop and one transaction: 100,000 inserts, about 3 ms each.
2. Its heartbeat stops until that ends: 317 seconds. The health limit is 30, so it would be restarted and roll back.

Inside 3, 100 batches:

1. Only 101 rows, so it is fast. But the lists are fixed: it does not know which partitions changed.

**1:50–2:00 · The open question.** Point at the last line, "That leaves one question". Stop there; do not scroll to chapter 4 yet.

1. That leaves one question.
2. Can Airflow keep the object count small, like batching,
3. and still know, per partition, what changed and what is done?

## Chapter 4 · Our route, step by step (1.5 min)

**要点：** 第一次说"我们"。先说一句话路线，然后用结构图下方的 Next 按钮逐步过：落地、debounce、batcher 和 lane、ledger 和 journal、下游、tick。每一步只读说明卡的第一句，再指一下卡右侧 Whose 下面的标签（Stock / Ours）。debounce 和 tick 是我们的临时件，而且碰时序，要主动说。完整的表格（七件工作加一行 Late events）折叠在下面，被问到再展开。每一步的说明卡下面有真实数据，重点让听众看到：大列表待在 state store 里，event 都很小。

Pause for a moment, then scroll. This is the first time you say "our".

**Overview.** Point at the whole diagram.

1. Our route in one sentence: Airflow schedules batches, and partitions travel as data.
2. Four Dags in a row after the landing. The partition list rides with the run, in the state store.

**Part 1 · Landing.** Click Next.

1. A producer sends one event per landing, listing its partitions. Stock. The box shows the real event: 2.4 MB.

**Part 2 · Debounce.** Click Next.

1. A debounce waits a short window, then emits one event. Ours, interim.
2. We wait because an engine job pays start-up per job. Its own event is 139 bytes: a pointer to the window key.

**Part 3 · Batcher.** Click Next. Point at the three lanes.

1. Claim picks the partitions whose inputs changed. Then one engine job runs.
2. A lane is one batcher run. Up to K run at once, here 3. Stock. The batch key holds every partition's input versions.

**Part 4 · Ledger.** Click Next.

1. The ledger is the only writer of the journal: done and failed per partition. Each entry is a dataset, a version and a marker.
2. No status view yet. Deep dive 3 shows how it handles two inputs.

**Part 5 · Downstream.** Click Next.

1. The next calculation gets one event per batch, and reads the batch key. That event is 130 bytes.

**Part 6 · Late events.** Click Next. Point at the small loops above the Dags.

1. If an event was left in a queue row a run had already consumed, a small sweep task rings a tick. Ours, interim. A tick is 14 bytes.

**Below the diagram.**

1. Our code is 323 lines. Airflow still starts runs, limits overlap and retries tasks.
2. The debounce and the ticks touch timing. We plan to propose both upstream.

## Chapter 5 · Deep dives (2 to 3 min: pick two or three)

**要点：** 深挖不用全讲，挑两三个，最好跟着听众的问题点。每个标签：念上面的问题，说下面一句话答案，指一下"实测"，最后说现状和下一步。推荐顺序：规模和成本、多输入一致性、负载下的延迟。可观测性用回放演示；重跑和失败处理留着答问。

**1 · Scale and cost.**

1. Airflow objects stay fixed: one landing is 6 events, 6 runs, 25 task instances.
2. What grows is the size of the partition lists we store. We plan to split those into small keys, under the store's 64 KB limit.
3. One landing took 93 seconds: 15 designed waits, 26 engine stand-in, 52 Airflow work.

**2 · Latency under load.** Click "Fast stand-in: lanes free", then "Slower stand-in: all 3 lanes busy". Say first: neither run used real Spark; they test how batching behaves at two engine speeds.

1. With lanes free, every landing was done within 60 seconds.
2. With lanes busy, the last job carried 24,000 partitions; its landings took 308 to 378 seconds.
3. Under load, small landings get their own reserved pool, routed before merging, so an adjustment never waits behind a big job. Designed, not built.

**3 · Multi-input consistency.** Point at the timeline: three landings (trades v5 at 0 s, positions v6 at 129 s, trades v6 at 162 s), two overlapping jobs, and the journal row at the bottom. The labels show what the designed rule would do on this recorded timeline: the trades job reads (6, 6) at start, so the journal ends on a set a job computed. Then point at the note under the chart: in the test as run, the current code copied versions at claim time and recorded (6, 6), a pair no job computed, for every partition.

1. Two inputs for one account can land seconds apart, and their jobs can overlap.
2. The rule: each job reads the newest of every input at start and reports that set. The ledger and the output keep only a set that is at least as new on every input, so the job that starts last wins.
3. It needs versioned inputs and atomic writes, which our feeds have. Not built yet.

**4 · Observability.** Use the replay.

1. Each batch is a run that turns red on failures. Per-partition state lives in the journal.
2. Today you read it as raw JSON. A status view is about one day.
3. Press Play at ×4. Pause at 16 s: "The 2.4 MB landing became one 139-byte pointer event."
4. Pause at 26 s: "Claim wrote one batch key with all 100,000 partitions. One engine task, not 100,000."
5. Pause at 73 s: "Downstream gets one 130-byte event. At 93 seconds the ledger writes done for all 100,000."

**5 · Rerun and backfill.**

1. Failed partitions can be re-queued with one call.
2. Recomputing a done partition on demand is not built: about 40 lines, one day.

**6 · Failure handling.**

1. Airflow retries first. After the last try, only partitions the engine names go to failed.
2. Tested on six partitions locally, never at 100,000.

## Chapter 6 · Requirements, checked, and asks (1 min)

**要点：** 一张表收尾：每条需求一个状态，点链接能跳回对应的深挖。主动说出最紧的 R6：要靠预留 pool、窗口长度和 Spark 启动时间，以及下一步要在 MWAA 上重跑。然后提两个要求，第三个问题留给对方，把话交出去。

**Point at:** the table, functional then non-functional, then the asks.

1. Here is every requirement with its status. Of the twelve scored ones, three are met, six are met with a condition, and three are partial.
2. The tightest one is R6, adjustment latency: it depends on the reserved pool, the window and Spark start-up. The next step is to repeat these runs on MWAA.
3. My asks: keep this as one option, and an MWAA 3.3.1 environment to repeat these runs.
4. And a question for you: should partition lineage live in Airflow's own tables, or in our journal plus a versioned output table?

## Likely questions

1. **Is this still orchestration, or a scheduler inside the scheduler?**

   中文要点：触发、串联、并发、task 重试都还是 Airflow 做；按分区的状态和血缘挪进了数据，随之挪进去的还有两条规则：哪些分区上车、最后一次重试后分区怎么处理。323 行里 36 行碰时序。debounce 和 tick 碰时序，是临时的，打算提上游，还没提。

   Airflow still triggers every run from asset events, chains the Dags, caps overlap with max_active_runs and a pool of K slots, retries tasks and shows run and task status. Per-partition status and lineage moved into data, and two rules moved with them: which partitions ride a job, and what happens to a partition after Airflow's last retry. Of 323 lines, 36 touch timing: two interim pieces, the debounce wait and the tick events. We plan to propose replacements upstream; nothing is filed yet.

2. **Native mapping over 100 batches did 100,000 in 38 seconds with no custom code. Why yours?**

   中文要点：方向对，但它跑的是固定的空任务，不知道哪些分区变了，不合并落地，也不记录每个分区的输入版本。这部分记账就是我们的 323 行。

   Batching is the right direction, and that run shows it. But it ran a fixed list of no-op tasks. It does not know which partitions changed, merge landings, or record input versions per partition. That bookkeeping is our 323 lines.

3. **MWAA gives up to five schedulers. Would native partitions be fast enough there?**

   中文要点：多调度器能加快完成，但登记 100k 个 key 要 1,184 秒，调度器帮不上；MWAA 也不能加索引。

   Extra schedulers help finish runs: a second one cut completion at 10,000 from 207 to 93 seconds. But even with indexes and two schedulers, registering the 100,000 keys took 1,184 seconds, and schedulers do not speed that up. MWAA gives no database access to add the indexes.

4. **Where does someone see the status of one partition?**

   中文要点：在 journal 里，也就是 state store 里的 done 和 failed。UI 只看得到 batch，按分区的视图还没做。MWAA 的 REST 限流每秒 10 次，不适合轮询。

   In the journal: the done and failed maps in the state store, per partition and input version, written only by the ledger. The Airflow UI shows batch runs and tasks. A per-partition view is not built. MWAA throttles REST at 10 requests per second, so polling the API is not a good way to show status.

5. **What happens when one bad partition fails the job?**

   中文要点：Airflow 先重试。最后一次还失败时，如果引擎点名了坏分区，只把它们标 failed，其余再跑一次。小规模本地验证过，100k 下没测。

   Airflow retries the engine task. If it fails again and the engine's error names the bad partitions, the ledger puts only them in failed and the rest run again in a new batch; if it names none, the whole batch fails. This worked in a small local run with six partitions. At 100,000 no task failed or retried, so it is untested at scale.

6. **Is 93 seconds what we would see in production?**

   中文要点：不是。93 秒里 41 秒是等待和替身。换成 60 秒窗口约 143 秒，再加上还没测过的真实 Spark 时间。

   No. 41 of the 93 seconds are the 10-second window, the 5-second ledger margin and the 26-second sleep for Spark. With the proposed 60-second window it would be about 143 seconds, and real Spark time is not measured.

7. **Could two inputs landing close together leave a wrong result?**

   中文要点：现在会：现在的代码在 claim 时复制版本，强制时序的 100,000 测试里每个分区都记了一个没有 job 算过的组合。修法已选定但还没做：启动时读最新，并报告读到的那一组版本；总账和结果表只接受每一项都不旧的组合。前提是输入有版本、写入是原子的，这两点都满足。

   Today, yes: the current code copies versions at claim time, and in a forced-timing test on 100,000 partitions it recorded a pair no job had computed, for every partition. The fix we chose: each job reads the newest version of every input when it starts, and reports the set it read. The journal and the output table keep only a set that is not older on any input, so a slower, older job cannot overwrite a newer one. Our inputs are versioned and written atomically, which this needs. Designed, not built.

8. **Will this run on MWAA, given the state store's 64 KB value limit?**

   中文要点：没验证。本地 task 写入超限只告警，REST 写入会被拒。我们本来就计划把总账和分区列表拆成小于 64 KB 的 key，大约 1 天，所以限额应该卡不住我们。有了环境先跑 1 万，再跑 10 万。

   Not checked. Locally, task writes over 64 KB pass with a warning, and REST writes over it are rejected. We already plan to split the journal and the partition lists into keys under 64 KB, about 1 day of work, so the limit should not block us. With an environment I would run 10,000 partitions first, then 100,000.

9. **Can an operator rerun one account on demand?**

   中文要点：现在只能重新排队失败的分区。已完成分区的按需重算还没做，应该是 claim 规则的一个小改动。原生方式可以按 run id 清掉单个 run，但没有 backfill，只能一个 key 调一次 API。

   Today, only failed partitions can be re-queued, with the inputs they failed on. Recomputing a done partition on demand is not built; it should be a small change to the claim rule, not made yet. Natively you clear that key's run by its run id, but there is no backfill for asset partitions: one API call per key.

10. **Isn't a 2.4 MB event a problem for Airflow?**

    中文要点：单独看不是压力：一行、一个请求 0.62 秒，Postgres 会压缩。代价在读它的地方：debounce 的 plan 和 sweep 最多读 5 条，forward 读整个窗口；审计日志还存一份；MWAA 的 REST 请求大小上限没查。真正的大头是 state store 里 4.4 MB 的 key 和 XCom。

    Not by itself. It is one row and one request: 0.62 seconds locally, and Postgres compresses it. 605 landing events hold 16.8 MB of JSON in a 5.3 MB table. The cost is in readers: the debounce's plan and sweep tasks read at most five landing events, and its forward task reads every event in the window, and Airflow's audit log keeps a copy of each REST body. MWAA's request size limit is not checked yet. The bigger load is elsewhere: the 4.4 MB state-store keys and the XComs, about 40 of the 52 seconds of Airflow work in that run.

11. **If upstream never takes the debounce or the attach-all-events change, this is permanent custom code in the scheduling path. Does the design still stand?**

    中文要点：如果上游都不收，"interim" 就改叫 "ours"，36 行留下：一个交给 stock sensor 的截止时间，两个私有 asset 上的小 tick task，每个都对得上调度器源码里的一个具体行为。正确性从来不靠它们是临时的：五个 100k 场景里没丢过事件，两个 margin sweep 一次没响，batcher tick 每个干活 run 多一个空 run。debounce 原则上也能去掉（队列行本来就会合并），代价是更多引擎启动，没测过。先把两个提案发出去，看上游怎么说。

    If neither ships, the honest label is "ours", not "interim", and the 36 lines stay: one deadline handed to a stock sensor and two short tick tasks on private assets, each tied to a specific Airflow behaviour we can point to in the scheduler source. The design's correctness never rested on them being temporary: in five 100,000-partition scenarios in a local environment, not MWAA, each run once, no event was stranded, the two margin sweeps never fired, and the batcher tick cost one empty run per working run. The debounce is also removable in principle, since Airflow's queue row already coalesces landings while the batcher is busy; dropping it would mean more engine start-ups rather than more code of ours, and we have not run without it. I would file both proposals before any MWAA run and bring the threads back.

12. **How much does this cost to build?**

    中文要点：现状：两个文件 323 行代码（六个 Dag + 账本规则），本地环境非 MWAA，五个 100k 场景各跑一次，零单测，两输入修法已定未做。到 MWAA 试点约 16 人天 ≈ 3 周（2–5 周）：两输入修法核心、64 KB 拆键、XCom 传 key、单测、纯 REST 驱动脚本在 MWAA 跑 10k/100k。到单个 calc 上生产累计约 50 人天 ≈ 10 周（6–17 周），其中约一半页面上没列：真实引擎接入、状态视图、保留策略、运维手册、小批预留池、业务日期。注意：页面各处 Next step 的天数加起来只有 12.5 天，同样的项重估是 22.5 天，页面是乐观端，被问到就这么说。引擎契约和版本化结果表归 calc 团队（粗估 3–5 天，由他们确认）。全部是自己估的，未经验证。

    Today it is a prototype: 323 lines in two files, six Dags and the journal rules, measured once per scenario at 100,000 partitions in a local environment, not MWAA, with no unit tests yet. To a first MWAA pilot I estimate about three weeks for one engineer, two to five: the two-input fix, splitting the journal under 64 KB, unit tests, and a REST-only run script, because nothing has run on MWAA. To production for one calc, about ten weeks in total, six to seventeen; roughly half of that is not on the page yet: real engine wiring, a status view, retention, runbooks, and the reserved pool for small adjustments. The two biggest uncertainties are that fix, which is model evidence until it is built and re-run at 100,000, and real Spark on MWAA, where every latency number today is a stand-in. Not ours: the engine contract and the versioned output table are the calc team's work, designed, not built, and I have not sized their side. These are my own estimates, unvalidated, for one engineer working serially; calendar time is longer because the environment, the calc team and any review run on other clocks.

## Do not say

1. 不说 "laptop"，说 "local environment, not MWAA"。
2. 93 秒不说 "one run"、"about a minute" 或 "production speed"。实际是 6 个 Dag run，而且 41 秒是等待和替身。要说 "one batcher run, one Spark job"。
3. 不说"原生分区在 MWAA 上不行"或"MWAA 不能加调度器"。MWAA 有 2 到 5 个调度器，卡点是登记 key 的时间和加不了索引。
4. 不说"全是原生的"、"时序全由 Airflow 决定"、"上游已经在做"。323 行是我们的，每个下游 Dag 还要一个 helper；debounce 和 tick 碰时序；上游什么都还没提交。
5. 不把 race 说成"100% 分区都会出错"的发生率，也不说"我们知道怎么修"。时序是强制的，修法未测。
6. 不说"100k 下失败处理没问题"、"能在 MWAA 跑"或"MWAA 接受 4.4 MB 的值"。这些都没测。
7. 不把"数据大小无所谓"、"能省 13 秒"、"Lambda 逐分区没问题"当事实讲。搬 JSON 占了 52 秒里的约 40 秒；XCom 的改动没试；没测过任何引擎。
8. 不用"落地、判断、计算、记录、通知"这种计算流程的说法，用编排词：trigger、DAG、dispatch、retry、status、lineage。不拿 region 粒度和账户粒度比。
9. 不拿这个方案和其他分区想法或调整通道做比较。
