HOW TO RUN THE PROGRAM

- Keep Protocol.py, server.py, worker.py and run_baseline.py in the same folder.

1. Open a terminal in that folder.

2. Run:

   python run_baseline.py

The script starts the server and four workers automatically.

WHAT TO EXPECT

The script runs the baseline test five times. In each run:
- The server distributes 1,000 addition tasks.
- Each of the four workers completes 250 tasks.
- The script checks the results, duplicate executions, acknowledgments,
  and worker shutdowns.

A successful run prints a line like:

Run 1/5: PASS - 1000 correct tasks; 250 per worker; no duplicate executions; all ACKs and shutdowns verified

After all five runs, expect:

Overall: PASS (5/5 passed)
Duration ms: min=..., median=..., max=...
Saved logs and results: ...

The duration values vary between runs and computers. They describe the
server's task-processing time.

To run the test only once:

python run_baseline.py --runs 1
