# Meeting material, 8 October 2026

One option for account-grain revenue on Airflow 3.3: can Airflow orchestrate 100,000 partitions?
Everything was measured in a local environment, not MWAA (Airflow 3.3.2, LocalExecutor, local Postgres); Spark is a sleep stand-in; each scenario ran once.

| File | What it is |
|---|---|
| [demo.html](demo.html) | The demo page: requirements, what an orchestrator does natively, why native breaks at 100,000 partitions, the route, six deep dives, requirements checked. |
| [walkthrough.html](walkthrough.html) | Table-by-table walkthrough: four recordings on the current code (`v2/recordings/kata_*.json`), each step with the rows that changed, a question and an answer. |
| [talk-track.md](talk-track.md) | Speaker notes for the demo page (English lines to say, Chinese notes, likely questions). |

The HTML files are self-contained; download them and open in a browser (GitHub shows their source, not the page).
The 100,000-partition numbers come from `v2/tools/exp100k.py` and `v2/recordings/exp100k_*.json`.
