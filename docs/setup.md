# Инструкция по настройке окружения

Если у вас что-то не получается, пишите свои вопросы в телеграм чат курса.

Для работы на курсе вам потребуется ноутбук с установленной операционной системой Linux. Мы рекомендуем использовать Ubuntu 20.04, так как там есть весь необходимый софт достаточно свежих версий.

Зарегистрируйтесь в проверяющей системе. (Если вы уже зарегистрировались в cpp.manytask.org на курсе по С++ или os.manytask.org на курсе по АКОС, вместо этого шага зайдите на https://db1.manytask.org/ и нажмите Login)

Заполните форму и зайдите в систему. (https://db1.manytask.org/ - secret code: `droptablestudent`.)
Сгенерируйте ssh ключ и добавьте его в систему.
```
> ssh-keygen -N "" -f ~/.ssh/id_rsa - сгенерирует пару файлов id_rsa и id_rsa.pub. (Если у вас уже есть ключ, этот шаг не нужен).
```
Скопируйте содержимое файла id_rsa.pub (`cat ~/.ssh/id_rsa.pub`) в (https://gitlab.manytask.org/profile/keys)
Проверьте, что все работает. Должно быть так:
```
ssh git@gitlab.manytask.org
PTY allocation request failed on channel 0
Welcome to GitLab, Fedor Korotkiy!
Connection to gitlab.manytask.org closed.
```

Склонируйте репозиторий с задачами:

```
git clone https://gitlab.com/kitaetoya/shad-db1
```

Настройте git.

Сконфигурируйте автора коммитов в git
```
git config --global user.name "Vasya Pupkin"
git config --global user.email vasya@pupkin.ru
```
Добавьте в git свой приватный репозиторий. Для этого запустите из директории репозитория команду:
`git remote add student git@gitlab.manytask.org:db1/students-fall-2023/XXXXXX.git`. Если в вашем нике есть точка, в url надо заменить их на `-`. Точно ссылку можно скопировать со страницы репозитория (Список репозиториев https://gitlab.manytask.org/)

Периодически мы будем обновлять репозиторий с задачами, поэтому перед каждым семинаром необходимо выполнять следующие 3 команды из вашего локального репозитория (shad-db1):
```
git stash
git pull
git stash pop
```
