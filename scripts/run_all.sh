if command -v uv &> /dev/null; then
    PKG_CMD="uv pip install -r requirements.txt --link-mode=copy"
    EXEC_CMD="uv run"
else
    PKG_CMD="pip install -r requirements.txt"
    EXEC_CMD="python3"
fi

# [Reasoning: Synchronize local virtual environment state with project manifest prior to daemon initialization.]
$PKG_CMD

# [Reasoning: Execute pipeline architecture concurrently via routed binary aliases.]
$EXEC_CMD kafka/producer_api.py &
$EXEC_CMD kafka/producer_rss.py &
$EXEC_CMD kafka/consumer_to_hdfs.py &
$EXEC_CMD scripts/spark_analysis.py --watch --interval 120 &
$EXEC_CMD dashboard/app.py &
