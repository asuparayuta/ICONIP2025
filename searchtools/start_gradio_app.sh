#!/bin/bash
# -*- coding: utf-8 -*-
#
# WholeBIF RDB Gradio App 起動スクリプト
#
# 使い方:
#   bash start_gradio_app.sh              # ローカルで起動
#   bash start_gradio_app.sh --share      # 公開リンクで起動
#   bash start_gradio_app.sh --auth       # 認証付きで起動
#

# ===== 設定 =====
APP_SCRIPT="gradio_wholebif_query_app_iconip.py"
ENV_FILE=".env.wholebif"

# 環境変数の読み込み
if [ -f "${ENV_FILE}" ]; then
    export $(cat ${ENV_FILE} | grep -v '^#' | xargs)
    echo "✅ 環境変数を読み込みました: ${ENV_FILE}"
else
    echo "⚠️  警告: ${ENV_FILE} が見つかりません"
    echo "   デフォルト設定で起動します"
fi

# ===== 起動オプションの解析 =====
SHARE_FLAG=""
AUTH_FLAG=""
HOST_FLAG=""
PORT_FLAG=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --share)
            SHARE_FLAG="--share"
            echo "📡 公開モード: 外部からアクセス可能なリンクを生成します"
            shift
            ;;
        --auth)
            if [ -z "$2" ] || [[ "$2" == --* ]]; then
                echo "❌ エラー: --auth には認証情報が必要です (例: --auth user:password)"
                exit 1
            fi
            AUTH_FLAG="--auth $2"
            echo "🔒 認証モード: ユーザー名とパスワードが必要です"
            shift 2
            ;;
        --host)
            if [ -z "$2" ] || [[ "$2" == --* ]]; then
                echo "❌ エラー: --host にはホスト名が必要です"
                exit 1
            fi
            HOST_FLAG="--host $2"
            shift 2
            ;;
        --port)
            if [ -z "$2" ] || [[ "$2" == --* ]]; then
                echo "❌ エラー: --port にはポート番号が必要です"
                exit 1
            fi
            PORT_FLAG="--port $2"
            shift 2
            ;;
        *)
            echo "❌ 不明なオプション: $1"
            echo ""
            echo "使い方:"
            echo "  bash start_gradio_app.sh [オプション]"
            echo ""
            echo "オプション:"
            echo "  --share              公開リンクを生成（外部アクセス可能）"
            echo "  --auth user:pass     認証を有効化"
            echo "  --host 0.0.0.0       ホストを指定"
            echo "  --port 7860          ポートを指定"
            echo ""
            echo "例:"
            echo "  bash start_gradio_app.sh --share"
            echo "  bash start_gradio_app.sh --share --auth admin:secret123"
            echo "  bash start_gradio_app.sh --host 0.0.0.0 --port 7860"
            exit 1
            ;;
    esac
done

# ===== Python環境チェック =====
echo ""
echo "=================================="
echo "環境チェック"
echo "=================================="

if ! command -v python3 &> /dev/null; then
    echo "❌ エラー: python3 が見つかりません"
    exit 1
fi

PYTHON_VERSION=$(python3 --version)
echo "✅ Python: ${PYTHON_VERSION}"

# 必要なパッケージチェック
echo ""
echo "必要なパッケージをチェック中..."

MISSING_PACKAGES=()

for pkg in pandas psycopg2 python-dotenv gradio; do
    if ! python3 -c "import ${pkg//-/_}" 2>/dev/null; then
        MISSING_PACKAGES+=($pkg)
    fi
done

if [ ${#MISSING_PACKAGES[@]} -gt 0 ]; then
    echo "⚠️  以下のパッケージがインストールされていません:"
    for pkg in "${MISSING_PACKAGES[@]}"; do
        echo "   - $pkg"
    done
    echo ""
    read -p "今すぐインストールしますか？ (yes/no): " install_confirm
    if [ "$install_confirm" = "yes" ]; then
        pip install "${MISSING_PACKAGES[@]}"
    else
        echo "❌ 必要なパッケージがインストールされていないため、終了します"
        exit 1
    fi
fi

echo "✅ すべてのパッケージがインストールされています"

# ===== PostgreSQL接続チェック =====
echo ""
echo "PostgreSQL接続チェック中..."

python3 << EOF
import psycopg2
import os

try:
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "wholebif_rdb"),
        user=os.getenv("POSTGRES_USER", "wholebif"),
        password=os.getenv("POSTGRES_PASSWORD", "")
    )
    print("✅ PostgreSQL接続成功")
    
    # テーブルチェック
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema='public' 
            ORDER BY table_name
        """)
        tables = [row[0] for row in cur.fetchall()]
    
    print(f"✅ テーブル数: {len(tables)}")
    required = ["circuits", "connections", "references_tbl"]
    for tbl in required:
        if tbl in tables:
            print(f"   ✅ {tbl}")
        else:
            print(f"   ❌ {tbl} (見つかりません)")
    
    conn.close()
except Exception as e:
    print(f"❌ PostgreSQL接続エラー: {e}")
    exit(1)
EOF

if [ $? -ne 0 ]; then
    exit 1
fi

# ===== アプリケーション起動 =====
echo ""
echo "=================================="
echo "Gradioアプリケーション起動"
echo "=================================="
echo ""

if [ ! -f "${APP_SCRIPT}" ]; then
    echo "❌ エラー: ${APP_SCRIPT} が見つかりません"
    exit 1
fi

echo "スクリプト: ${APP_SCRIPT}"
echo "オプション: ${SHARE_FLAG} ${AUTH_FLAG} ${HOST_FLAG} ${PORT_FLAG}"
echo ""
echo "起動中..."
echo ""

python3 ${APP_SCRIPT} ${SHARE_FLAG} ${AUTH_FLAG} ${HOST_FLAG} ${PORT_FLAG}
