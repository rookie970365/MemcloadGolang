# MemcloadGolang
Go-версия реализации загрузчика логов в memcached 
***
Сĸрипт парсит и заливает в memcached поминутную
выгрузĸу логов треĸера установленных приложений. Ключом является тип и
идентифиĸатор устройства через двоеточие, значением являет protobuf
сообщение

### Ссылĸи на tsv.gz лог-файлы:
https://cloud.mail.ru/public/2hZL/Ko9s8R9TA

https://cloud.mail.ru/public/DzSX/oj8RxGX1A

https://cloud.mail.ru/public/LoDo/SfsPEzoGc
### Пример строки лог-файла:
idfa&nbsp;&nbsp;&nbsp;&nbsp;5rfw452y52g2gq4g&nbsp;&nbsp;&nbsp;&nbsp;55.55&nbsp;&nbsp;&nbsp;&nbsp;42.42&nbsp;&nbsp;&nbsp;&nbsp;5423,43,567,3,7,23

gaid&nbsp;&nbsp;&nbsp;&nbsp;6rfw452y52g2gq4g&nbsp;&nbsp;&nbsp;&nbsp;55.55&nbsp;&nbsp;&nbsp;&nbsp;42.42&nbsp;&nbsp;&nbsp;&nbsp;6423,43,567,3,7,23
## Запуск
```sh
go run main.go 
```
