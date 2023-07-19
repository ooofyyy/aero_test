import clickhouse_connect
import pytz
import requests
import time

import datetime as dt

class cannabis_loader():
    def __init__(self):
        #для конфигурации некоторых моментов в одном удобном месте
        self.trgt_table = '<INSERT TABLE NAME HERE>'
        self.lof_file = '<INSERT LOG TXT FILE PATH HERE>'
        self.creds_path = '<INSERT CREDS TXT FILE PATH HERE>'

    def logger(self,message):
        #простой логгер который пишет в файл
        stamp = dt.datetime.now(pytz.timezone('GMT+0')).strftime('%y-%m-%d %H:%M:%S')
        with open(self.lof_file,'a') as f:
            f.write(f'{stamp} --- {message} \n')
        return
    
    def connector(self):
        #выгрузка данных из api, в случае неудачи осуществляется несколько попыток
        retries = 0
        while retries < 3:
            resp = requests.get('https://random-data-api.com/api/cannabis/random_cannabis?size=10')
            if resp.status_code == 200:
                return resp.json()
            else:
                #задержка на 10 20 40 секунд для нескольких попыток 
                time.sleep(10 * 2**retries)
                retries += 1
        #если цикл while завершился значит получить данные не удалось
        return []

    def get_connect(self):
        #что бы спрятать данные для аутентификации из кода
        with open(self.creds_path,'r') as f:
            cont = f.read()
            user = cont.split(',')[0]
            pasw = cont.split(',')[1]
            host = cont.split(',')[2]
            port = cont.split(',')[3]
            ch_client = clickhouse_connect.get_client(host=host,
                                                      port=port,
                                                      username=user,
                                                      password=pasw)
        return ch_client

    def write_to_ch(self,resp):
        '''
        Функция пишет данные в КХ и запускает OPTIMIZE
        OPTIMIZE нужен для того что бы убрать дублирующиеся данные в таблице
        Два момента:
        1. Если таблица большая то запускать OPTIMIIZE не выгодно
        2. КХ сам смержит через какое-то время дублирующиеся строки
        Тут надо выбирать
        Если таблица большая и выгрузка, например, происходит ночью, а данные нужны утром, то 
        OPTIMIZE можно не делать
        Если данные нужны прям сразу, то OPTIMIZE нужен
        Но можно реализовать и третий путь, где мы не делаем ReplacingMergeTree, а используем
        обычный MergeTree, тогда надо добавить фильтр, который будет проверять если среди выгруженых данных то,
        что уже записано в таблицу и не писать это повторно
        Плюсом ещё неплохо бы до конца знать что конкретно за данные и могут ли менятся данные у конкретного id
        Если это так, то надо знать нужно ли нам хранить исторические данные или нет (но как правило нужно :D)
        '''
        ch_client = self.get_connect()
        columns = list(resp[0].keys())
        rows = [list(x.values()) for x in resp]
        ch_client.insert(self.trgt_table, rows, column_names=columns)
        ch_client.command(f'OPTIMIZE TABLE {self.trgt_table}')
        return

    def main(self):
        resp = self.connector()
        if resp:
            #если получили ответ то пишем и логируем успех и сколько данных отдало api
            self.write_to_ch(resp)
            self.logger(f'Success, samples writed - {len(resp)}')
        else:
            #если не получили, то логируем проблемку
            #также логирование можно настроить в КХ
            self.logger('Server did not respond')
        return
    
if __name__ == '__main__':
    cannabis_loader().main()