# 🔐 **Пошаговая настройка SSH для SourceCraft**

## ✅ **ШАГ 1: SSH ключ создан!**

Ваш SSH ключ уже создан и скопирован в буфер обмена.

## 📱 **ШАГ 2: Добавление ключа в SourceCraft**

### **1. Откройте SourceCraft в браузере:**
```
https://sourcecraft.dev
```

### **2. Войдите в свой аккаунт**

### **3. Перейдите в настройки:**
- Нажмите на аватар/имя пользователя в правом верхнем углу
- Выберите **"Settings"** или **"Настройки"**

### **4. Найдите раздел SSH Keys:**
- В левом меню найдите **"SSH Keys"** или **"SSH ключи"**
- Нажмите на этот раздел

### **5. Добавьте новый ключ:**
- Нажмите кнопку **"Add SSH Key"** или **"Добавить SSH ключ"**
- Дайте ключу название, например: `"ETL Assistant - Desktop"`
- В поле **"Key"** нажмите **Ctrl+V** (ключ уже в буфере обмена)
- Нажмите **"Save"** или **"Сохранить"**

## ✅ **ШАГ 3: Проверка и загрузка кода**

После добавления SSH ключа:

### **Выполните в терминале:**
```bash
git push -u origin main
```

### **Если команда выполнится успешно - увидите:**
```
Enumerating objects: XX, done.
Counting objects: 100% (XX/XX), done.
...
To ssh://ssh.sourcecraft.dev/graphxxl/digital-ing.git
 * [new branch]      main -> main
Branch 'main' set up to track remote branch 'main' from 'origin'.
```

## 🚀 **ШАГ 4: Настройка переменных окружения**

В SourceCraft Dashboard:
1. Найдите ваш проект **"digital-ing"**
2. Перейдите в **Settings → Environment Variables**
3. Добавьте переменные из файла `SOURCECRAFT_ENV_VARS.md`

## 🎯 **ШАГ 5: Развертывание**

1. В SourceCraft Dashboard нажмите **"Deploy"**
2. Ждите 5-10 минут
3. Получите production URL!

---

## 🆘 **Если что-то не работает:**

### **Ошибка "Permission denied":**
- Убедитесь, что SSH ключ добавлен в SourceCraft
- Проверьте, что скопировали весь ключ (от `ssh-rsa` до `@sourcecraft.dev`)

### **Ошибка "Host key verification failed":**
- Выполните: `ssh-keyscan ssh.sourcecraft.dev >> ~/.ssh/known_hosts`

### **Нужна помощь:**
- Проверьте, что находитесь в правильной папке проекта
- Убедитесь, что Git настроен: `git config --global user.email "your@email.com"`

---

## 🎊 **Результат:**

После успешной настройки получите:
- 🌐 **Frontend:** https://digital-ing-etl-assistant.sourcecraft.dev
- 🔗 **Backend:** https://backend-digital-ing-etl-assistant.sourcecraft.dev
- 📊 **Готовый к демонстрации жюри production сервис!**
