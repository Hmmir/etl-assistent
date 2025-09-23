# 🚀 **GitHub → SourceCraft Deployment Guide**

## 🎯 **Стратегия: Двухэтапное развертывание**

Используем GitHub как промежуточное хранилище для стабильной загрузки в SourceCraft.

---

## 📋 **ШАГ 1: Создание репозитория на GitHub**

### **1. Откройте GitHub:**
```
https://github.com
```

### **2. Создайте новый репозиторий:**
- Нажмите зеленую кнопку **"New repository"**
- **Repository name:** `etl-assistant-moscow`
- **Description:** `ETL Assistant для хакатона ДИТ Москвы` 
- **Visibility:** `Public`
- **❌ НЕ добавляйте:** README, .gitignore, license (у нас уже есть)
- Нажмите **"Create repository"**

### **3. Скопируйте URL репозитория:**
```
https://github.com/ВАШ_USERNAME/etl-assistant-moscow.git
```

---

## 🚀 **ШАГ 2: Автоматическая загрузка (выполню я)**

После создания GitHub репозитория выполню:

```bash
# Добавление GitHub remote
git remote add github https://github.com/ВАШ_USERNAME/etl-assistant-moscow.git

# Загрузка кода в GitHub
git push -u github main

# Проверка загрузки
git log --oneline -5
```

---

## 🔄 **ШАГ 3: Импорт в SourceCraft**

### **1. В SourceCraft Dashboard:**
- **Create repository** → **"Migrate existing"**

### **2. Заполнение формы импорта:**
```
Source repository URL: https://github.com/ВАШ_USERNAME/etl-assistant-moscow
Authentication: Public repository (без токена)
Name: etl-assistant-moscow
Visibility: Public
Synchronization: main (для автообновлений)
```

### **3. Нажать "Begin migration"**

---

## 🌐 **ШАГ 4: Production развертывание**

### **Автоматически получим:**
- 📦 **Код в SourceCraft:** https://sourcecraft.dev/graphxxl/etl-assistant-moscow
- 🌐 **Frontend:** https://etl-assistant-moscow.sourcecraft.dev
- 🔗 **Backend:** https://backend-etl-assistant-moscow.sourcecraft.dev
- 📊 **Airflow:** https://airflow-etl-assistant-moscow.sourcecraft.dev

---

## ✅ **Преимущества этого подхода:**

1. **🔗 Стабильность:** GitHub надежнее для больших репозиториев
2. **🔄 Синхронизация:** Автообновления из GitHub в SourceCraft
3. **📦 Резервирование:** Код сохранен и в GitHub, и в SourceCraft  
4. **🚀 Скорость:** Быстрый импорт вместо медленного push
5. **🌐 Доступность:** Репозиторий доступен на двух платформах

---

## 🎯 **Текущий статус:**

- ✅ Локальный репозиторий готов
- ⏳ Ожидание создания GitHub репозитория
- 🔄 Готов к автоматической загрузке
- 🚀 Готов к импорту в SourceCraft

**Создавайте GitHub репозиторий и сообщайте URL!**
