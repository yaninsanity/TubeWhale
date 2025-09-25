#!/bin/bash
# TubeWhale Multilingual Locale Compilation Script
# Compiles all translation files for production deployment

set -e

echo "🌍 TubeWhale Multilingual Locale Compilation"
echo "=============================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

echo -e "${BLUE}📁 Working directory: $PROJECT_ROOT${NC}"

# Check if Django is available
if ! python -c "import django" 2>/dev/null; then
    echo -e "${RED}❌ Django not found. Please ensure you're in the correct virtual environment.${NC}"
    exit 1
fi

# Check if manage.py exists
if [ ! -f "manage.py" ]; then
    echo -e "${RED}❌ manage.py not found. Please run this script from the project root.${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Django environment validated${NC}"

# Supported languages
LANGUAGES=("en" "zh-hans" "zh-hant" "ja" "ko" "es" "fr" "de" "pt" "ru" "ar" "hi")
TOTAL_LANGUAGES=${#LANGUAGES[@]}

echo -e "${BLUE}🔍 Found $TOTAL_LANGUAGES supported languages: ${LANGUAGES[*]}${NC}"

# Create locale directory if it doesn't exist
if [ ! -d "locale" ]; then
    echo -e "${YELLOW}📁 Creating locale directory...${NC}"
    mkdir -p locale
fi

# Step 1: Extract translatable strings (makemessages)
echo -e "${BLUE}📝 Step 1: Extracting translatable strings...${NC}"

# Create .po files for all languages
for lang in "${LANGUAGES[@]}"; do
    echo -e "${YELLOW}  • Processing language: $lang${NC}"
    
    # Create language directory if it doesn't exist
    mkdir -p "locale/$lang/LC_MESSAGES"
    
    # Extract messages for this language
    if python manage.py makemessages -l "$lang" --no-wrap --no-obsolete; then
        echo -e "${GREEN}    ✅ Extracted messages for $lang${NC}"
    else
        echo -e "${RED}    ❌ Failed to extract messages for $lang${NC}"
    fi
done

# Step 2: Extract JavaScript translatable strings
echo -e "${BLUE}📝 Step 2: Extracting JavaScript translatable strings...${NC}"

for lang in "${LANGUAGES[@]}"; do
    echo -e "${YELLOW}  • Processing JavaScript for language: $lang${NC}"
    
    if python manage.py makemessages -l "$lang" -d djangojs --no-wrap --no-obsolete; then
        echo -e "${GREEN}    ✅ Extracted JavaScript messages for $lang${NC}"
    else
        echo -e "${RED}    ❌ Failed to extract JavaScript messages for $lang${NC}"
    fi
done

# Step 3: Compile translation files (compilemessages)
echo -e "${BLUE}🏗️  Step 3: Compiling translation files...${NC}"

COMPILED_COUNT=0
FAILED_COUNT=0

for lang in "${LANGUAGES[@]}"; do
    echo -e "${YELLOW}  • Compiling language: $lang${NC}"
    
    # Check if .po file exists
    PO_FILE="locale/$lang/LC_MESSAGES/django.po"
    JS_PO_FILE="locale/$lang/LC_MESSAGES/djangojs.po"
    
    if [ -f "$PO_FILE" ]; then
        if python manage.py compilemessages -l "$lang"; then
            echo -e "${GREEN}    ✅ Compiled messages for $lang${NC}"
            ((COMPILED_COUNT++))
        else
            echo -e "${RED}    ❌ Failed to compile messages for $lang${NC}"
            ((FAILED_COUNT++))
        fi
    else
        echo -e "${YELLOW}    ⚠️  No .po file found for $lang, skipping...${NC}"
    fi
done

# Step 4: Validate compiled files
echo -e "${BLUE}🔍 Step 4: Validating compiled files...${NC}"

VALIDATED_COUNT=0
for lang in "${LANGUAGES[@]}"; do
    MO_FILE="locale/$lang/LC_MESSAGES/django.mo"
    if [ -f "$MO_FILE" ]; then
        SIZE=$(stat -f%z "$MO_FILE" 2>/dev/null || stat -c%s "$MO_FILE" 2>/dev/null || echo "0")
        if [ "$SIZE" -gt 0 ]; then
            echo -e "${GREEN}  ✅ $lang: $MO_FILE ($SIZE bytes)${NC}"
            ((VALIDATED_COUNT++))
        else
            echo -e "${RED}  ❌ $lang: $MO_FILE is empty${NC}"
        fi
    else
        echo -e "${YELLOW}  ⚠️  $lang: No compiled file found${NC}"
    fi
done

# Step 5: Generate locale statistics
echo -e "${BLUE}📊 Step 5: Generating locale statistics...${NC}"

# Create statistics file
STATS_FILE="locale/translation_stats.json"
echo "{" > "$STATS_FILE"
echo "  \"compilation_date\": \"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\"," >> "$STATS_FILE"
echo "  \"total_languages\": $TOTAL_LANGUAGES," >> "$STATS_FILE"
echo "  \"compiled_languages\": $COMPILED_COUNT," >> "$STATS_FILE"
echo "  \"failed_languages\": $FAILED_COUNT," >> "$STATS_FILE"
echo "  \"validated_languages\": $VALIDATED_COUNT," >> "$STATS_FILE"
echo "  \"supported_languages\": [" >> "$STATS_FILE"

for i in "${!LANGUAGES[@]}"; do
    lang="${LANGUAGES[$i]}"
    if [ $i -eq $((TOTAL_LANGUAGES - 1)) ]; then
        echo "    \"$lang\"" >> "$STATS_FILE"
    else
        echo "    \"$lang\"," >> "$STATS_FILE"
    fi
done

echo "  ]," >> "$STATS_FILE"
echo "  \"locale_files\": {" >> "$STATS_FILE"

FIRST=true
for lang in "${LANGUAGES[@]}"; do
    MO_FILE="locale/$lang/LC_MESSAGES/django.mo"
    if [ -f "$MO_FILE" ]; then
        SIZE=$(stat -f%z "$MO_FILE" 2>/dev/null || stat -c%s "$MO_FILE" 2>/dev/null || echo "0")
        if [ "$FIRST" = true ]; then
            FIRST=false
        else
            echo "," >> "$STATS_FILE"
        fi
        echo -n "    \"$lang\": {\"file\": \"$MO_FILE\", \"size\": $SIZE}" >> "$STATS_FILE"
    fi
done

echo "" >> "$STATS_FILE"
echo "  }" >> "$STATS_FILE"
echo "}" >> "$STATS_FILE"

# Step 6: Generate locale summary report
echo -e "${BLUE}📋 Step 6: Generating summary report...${NC}"

REPORT_FILE="locale/COMPILATION_REPORT.md"
cat > "$REPORT_FILE" << EOF
# TubeWhale Multilingual Compilation Report

**Compilation Date:** $(date -u +"%Y-%m-%d %H:%M:%S UTC")
**Project:** TubeWhale YouTube Analysis Platform
**System:** English-First Multilingual Support

## Summary

- **Total Languages:** $TOTAL_LANGUAGES
- **Successfully Compiled:** $COMPILED_COUNT
- **Failed Compilations:** $FAILED_COUNT
- **Validated Files:** $VALIDATED_COUNT
- **Success Rate:** $(( COMPILED_COUNT * 100 / TOTAL_LANGUAGES ))%

## Supported Languages

| Language Code | Language Name | Status | File Size |
|---------------|---------------|--------|-----------|
EOF

# Add language details to report
for lang in "${LANGUAGES[@]}"; do
    case $lang in
        "en") name="English" ;;
        "zh-hans") name="简体中文 (Simplified Chinese)" ;;
        "zh-hant") name="繁體中文 (Traditional Chinese)" ;;
        "ja") name="日本語 (Japanese)" ;;
        "ko") name="한국어 (Korean)" ;;
        "es") name="Español (Spanish)" ;;
        "fr") name="Français (French)" ;;
        "de") name="Deutsch (German)" ;;
        "pt") name="Português (Portuguese)" ;;
        "ru") name="Русский (Russian)" ;;
        "ar") name="العربية (Arabic)" ;;
        "hi") name="हिन्दी (Hindi)" ;;
        *) name="Unknown" ;;
    esac
    
    MO_FILE="locale/$lang/LC_MESSAGES/django.mo"
    if [ -f "$MO_FILE" ]; then
        SIZE=$(stat -f%z "$MO_FILE" 2>/dev/null || stat -c%s "$MO_FILE" 2>/dev/null || echo "0")
        if [ "$SIZE" -gt 0 ]; then
            STATUS="✅ Compiled"
            SIZE_STR="${SIZE} bytes"
        else
            STATUS="❌ Empty"
            SIZE_STR="0 bytes"
        fi
    else
        STATUS="⚠️ Missing"
        SIZE_STR="N/A"
    fi
    
    echo "| $lang | $name | $STATUS | $SIZE_STR |" >> "$REPORT_FILE"
done

cat >> "$REPORT_FILE" << EOF

## File Structure

\`\`\`
locale/
├── translation_stats.json
├── COMPILATION_REPORT.md
EOF

for lang in "${LANGUAGES[@]}"; do
    echo "├── $lang/" >> "$REPORT_FILE"
    echo "│   └── LC_MESSAGES/" >> "$REPORT_FILE"
    if [ -f "locale/$lang/LC_MESSAGES/django.po" ]; then
        echo "│       ├── django.po" >> "$REPORT_FILE"
    fi
    if [ -f "locale/$lang/LC_MESSAGES/django.mo" ]; then
        echo "│       ├── django.mo" >> "$REPORT_FILE"
    fi
    if [ -f "locale/$lang/LC_MESSAGES/djangojs.po" ]; then
        echo "│       ├── djangojs.po" >> "$REPORT_FILE"
    fi
    if [ -f "locale/$lang/LC_MESSAGES/djangojs.mo" ]; then
        echo "│       └── djangojs.mo" >> "$REPORT_FILE"
    fi
done

cat >> "$REPORT_FILE" << EOF
\`\`\`

## Next Steps

1. **Test Language Switching:** Verify all languages work correctly in the application
2. **Update Missing Translations:** Review .po files for untranslated strings
3. **Deploy to Production:** Copy compiled .mo files to production environment
4. **Monitor Performance:** Check translation loading performance in production

## Commands Used

\`\`\`bash
# Extract translatable strings
python manage.py makemessages -l <lang> --no-wrap --no-obsolete

# Extract JavaScript strings
python manage.py makemessages -l <lang> -d djangojs --no-wrap --no-obsolete

# Compile translation files
python manage.py compilemessages -l <lang>
\`\`\`

---
Generated by TubeWhale Multilingual Compilation System
EOF

# Final summary
echo ""
echo "=============================================="
echo -e "${GREEN}🎉 Multilingual Compilation Complete!${NC}"
echo "=============================================="
echo -e "${BLUE}📊 Summary:${NC}"
echo -e "   • Total Languages: $TOTAL_LANGUAGES"
echo -e "   • Successfully Compiled: ${GREEN}$COMPILED_COUNT${NC}"
echo -e "   • Failed Compilations: ${RED}$FAILED_COUNT${NC}"
echo -e "   • Validated Files: ${GREEN}$VALIDATED_COUNT${NC}"
echo -e "   • Success Rate: ${GREEN}$(( COMPILED_COUNT * 100 / TOTAL_LANGUAGES ))%${NC}"
echo ""
echo -e "${BLUE}📁 Generated Files:${NC}"
echo -e "   • Statistics: ${YELLOW}$STATS_FILE${NC}"
echo -e "   • Report: ${YELLOW}$REPORT_FILE${NC}"
echo ""

if [ $FAILED_COUNT -gt 0 ]; then
    echo -e "${YELLOW}⚠️  Note: $FAILED_COUNT language(s) failed to compile. Check the output above for details.${NC}"
    exit 1
else
    echo -e "${GREEN}✅ All supported languages compiled successfully!${NC}"
    echo -e "${BLUE}🚀 Ready for production deployment.${NC}"
fi

echo ""