#!/bin/bash
# Конвертирует vegeta histogram в формат HDR Histogram

echo "Value   Percentile   TotalCount 1/(1-Percentile)"
echo ""

# Парсим вывод vegeta и конвертируем
awk '
/\[/ {
    gsub(/\[/, "", $1)
    gsub(/ms\]/, "", $2)
    value = $2 * 1000
    if (value == 0) value = 0.001
    print sprintf("%.3f     %.6f", value, 0)
}
' $1
