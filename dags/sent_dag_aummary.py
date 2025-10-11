from airflow.operators.dummy import DummyOperator
from telegram_notifier import TelegramNotifier


def _send_dag_summary(**context):
    parse_result = context["task_instance"].xcom_pull(task_ids="parse_auctions_and_save")

    dag_id = context["dag"].dag_id
    execution_date = context["execution_date"]
    start_time = context["dag_run"].start_date
    end_time = context["task_instance"].start_date

    duration = (end_time - start_time).total_seconds() if start_time else 0
    mins, secs = divmod(int(duration), 60)

    total_lots = parse_result.get("total_lots", "N/A") if parse_result else "N/A"

    message = (
        f"✅ *DAG успешно завершён!*\n\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Дата запуска:* `{execution_date}`\n"
        f"*Обработано лотов:* `{total_lots}`\n"
        f"*Время выполнения:* `{mins} мин {secs} сек`"
    )

    notifier = TelegramNotifier()
    notifier.notify({**context, "exception": None})