# 🎯 **Настройка нового репозитория SourceCraft**

## 📝 **Заполнение формы создания репозитория:**

### **✅ Поля для заполнения:**
- **Owner:** `graphxxl` (уже выбрано)
- **Name:** `etl-assistant-moscow` 
- **Visibility:** `Public` (для демонстрации жюри)
- **Synchronization:** `оставить пустым`

### **🌐 Результат:**
Получится репозиторий: `https://sourcecraft.dev/graphxxl/etl-assistant-moscow`

---

## 🚀 **После создания репозитория:**

### **Автоматические команды:**
```bash
# 1. Обновляем remote URL
git remote set-url origin ssh://ssh.sourcecraft.dev/graphxxl/etl-assistant-moscow.git

# 2. Загружаем код
git push -u origin main

# 3. Проверяем успешную загрузку
git log --oneline -3
```

### **🔧 Обновление конфигурации:**
Автоматически обновятся URLs в:
- `sourcecraft.yml`
- `README.md` 
- `SOURCECRAFT_ENV_VARS.md`

---

## 🎯 **Production URLs после деплоя:**
- 🌐 **Frontend:** `https://etl-assistant-moscow.sourcecraft.dev`
- 🔗 **Backend:** `https://backend-etl-assistant-moscow.sourcecraft.dev`
- 📊 **Airflow:** `https://airflow-etl-assistant-moscow.sourcecraft.dev`

---

## 📋 **Следующие шаги:**
1. ✅ Создать репозиторий в SourceCraft
2. 🚀 Автоматическая загрузка кода
3. ⚙️ Настройка переменных окружения
4. 🎯 Развертывание production системы
5. 🏆 Демонстрация жюри!

**🎊 Готово к победе на хакатоне ДИТ Москвы!**
