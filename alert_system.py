import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io
import os
from dotenv import load_dotenv
import pandas as pd
import pandahouse as ph
from airflow.decorators import dag, task
from datetime import datetime, timedelta


chat_id = int(os.getenv('CHAT_ID'))
my_token = os.getenv('MY_TOKEN')
bot = telegram.Bot(token=my_token)

# Подключение к базе
load_dotenv()
connection = {
    'host': os.getenv('HOST'),
    'password': os.getenv('PASSWORD'),
    'user': os.getenv('USER')
}

# Дефолтные параметры для тасков
default_args = {
    'owner': os.getenv('OWNER'),
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 5, 28)
}

# Интервал запуска DAG
schedule_interval = '*/15 * * * *'

# Запрос на получение данных из базы
query = '''
    WITH src1 as (
        SELECT
            toStartOfFifteenMinutes(time) as ts,
            toDate(time) as date,
            formatDateTime(ts, '%R') as hm,
            uniqExact(user_id) as users_feed,
            countIf(f.user_id, action='view') as views,
            countIf(f.user_id, action='like') as likes
        FROM 
            simulator_20240420.feed_actions as f
        WHERE time >= today() - 1 AND time < toStartOfFifteenMinutes(now())
        GROUP BY ts, date, hm
    ),
    src2 as (
        SELECT
            toStartOfFifteenMinutes(time) as ts,
            uniqExact(user_id) as users_message, 
            count(user_id) as messages
        FROM 
            simulator_20240420.message_actions
        WHERE time >= today() - 1 AND time < toStartOfFifteenMinutes(now())
        GROUP BY ts
    )
    SELECT
        ts, date, hm,
        users_feed,
        views,likes,
        round(likes/views, 2) as ctr,
        users_message,
        messages
    FROM src1 left join src2 on src1.ts = src2.ts
    ORDER BY ts
'''

# Ссылка на дашборд
url = os.getenv('URL')



def check_anomaly(df, metric, a=3, n=5):
    """Функция реализует алгоритм поиска аномалий в данных (межквартильный размах)

    Args:
        df (DataFrame): Датафрейм, сформированный для исследуемой метрики
        metric str):  Исследуемая метрика
        a (int, optional): Коэффициент, определяющий ширину интервала. Defaults to 3.
        n (int, optional): Количество временных периодов. Defaults to 5.

    Returns:
        int: Флаг: есть аномалия -1, нет аномалии -0
        DataFrame: датафрейм с добавленными столбцами границ для метрики
    """
    
    # Отображаем все значения за сегодня, чтобы оценить поведение метрики в течение дня
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    # Вычисляем значение межквартильного размаха
    df['iqr'] = df['q75'] - df['q25']
    # Вычисляем значения границ
    df['up'] = df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']
    # Сглаживаем границы, текущая 15-минутка будет посередине окна
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()

    # Проверяем значение метрики
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0

    return is_alert, df

    
def check_anomaly_ctr(df, metric):
    """ Функция задает границы для метрики CTR

    Args:
        df (DataFrame): Датафрейм, сформированный для исследуемой метрики
        metric str):  Исследуемая метрика (CTR)

    Returns:
        int: Флаг: есть аномалия -1, нет аномалии -0
        DataFrame: датафрейм с добавленными столбцами границ для метрики
    """
    # Устанавливаем значения границ
    df['up'] = 0.23
    df['low'] = 0.19
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    
    return is_alert, df

    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def get_alert_report_ouman():
    
    @task
    def get_alert():
        """Таск считывает данные из базы, для каждой метрики формирует датафрейм,
        применяет соответствующий алгоритм поиска аномалий.
        Если аномалия выявлена, таск отправляет сообщение в Телеграм-канал
        и отрисовывает график метрики.
        """
        # Считываем данные из базы
        data = ph.read_clickhouse(query=query, connection=connection)
        # Список контролируемых метрик
        metrics_list = ['users_feed', 'views', 'likes', 'ctr', 'users_message', 'messages']
        
        for metric in metrics_list:
            print(metric)
            # Формируем датафрейм для метрики
            df = data[['ts', 'date', 'hm', metric]].copy()
            # Применяем алгоритм поиска аномалий
            if metric == 'ctr':
                is_alert, df = check_anomaly_ctr(df, metric)
            else:
                is_alert, df = check_anomaly(df, metric)
            # Текст сообщения об алерте
            if is_alert == 1:
                msg = f'''Метрика {metric.capitalize()}:\nТекущее значение {df[metric].iloc[-1]:.2f}
Отклонение от предыдущего значения {1-df[metric].iloc[-1]/df[metric].iloc[-2]:.2%}
Дашборд с метриками: {url}
'''
                # Задаем параметры графиков
                sns.set(rc={'figure.figsize': (16, 10)})
                plt.tight_layout() 

                # Строим метрику
                ax = sns.lineplot(x=df['ts'], y=df[metric], label='metric')
                # Строим границы
                ax = sns.lineplot(x=df['ts'], y=df['up'], label='up')
                ax = sns.lineplot(x=df['ts'], y=df['low'], label='low')

                ax.set(xlabel='time')
                ax.set(ylabel=metric)
                ax.set_title(metric)
                ax.set(ylim=(0, None))
                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = f'{metric}.png'
                plt.close()

                # Отправляем сообщение
                bot.sendMessage(chat_id=chat_id, text=msg)
                # Отправляем график
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
                
    get_alert()

get_alert_report_ouman = get_alert_report_ouman()