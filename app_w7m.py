import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import json
from datetime import datetime, timedelta
import numpy as np

def carregar_dados():
    """
    Carrega e preprocessa todos os arquivos CSV necessários para a análise.
    Aplica limpeza e renomeação de colunas logo no início conforme especificado.
    
    Returns:
        tuple: DataFrames limpos e transformados
    """
    try:
        # ==================== CARREGAMENTO DOS DADOS ORIGINAIS ====================
        df_transacoes = pd.read_csv('data_new/store_transaction.csv')
        df_user_product = pd.read_csv('data_new/user_product.csv')  # Assumindo que este é o df_store_product mencionado
        df_product = pd.read_csv('data_new/product.csv')
        df_boost_trans = pd.read_csv('data_new/boost_transaction.csv')
        df_boost = pd.read_csv('data_new/boost.csv')
        df_partner = pd.read_csv('data_new/partner.csv')
        df_campaign = pd.read_csv('data_new/campaign.csv')
        df_campaign_user = pd.read_csv('data_new/campaign_user.csv')
        df_campaign_quest = pd.read_csv('data_new/campaign_quest.csv')
        df_reward = pd.read_csv('data_new/reward.csv')
        df_subscription = pd.read_csv('data_new/subscription.csv')
        df_user = pd.read_csv('data_new/user.csv')
        df_user_partner_score = pd.read_csv('data_new/user_partner_score.csv')
        
        # ==================== TRANSFORMAÇÕES OBRIGATÓRIAS ====================
        
        # 1. df_transacoes
        colunas_descartar = ['Wallet ID', 'Updated At']
        for col in colunas_descartar:
            if col in df_transacoes.columns:
                df_transacoes = df_transacoes.drop(columns=[col])
        
        if 'ID' in df_transacoes.columns:
            df_transacoes = df_transacoes.rename(columns={'ID': 'Transaction ID'})
        
        if 'Created At' in df_transacoes.columns:
            df_transacoes['Transaction Created At'] = pd.to_datetime(df_transacoes['Created At'], errors='coerce')
        
        # 2. df_user_product (df_store_product)
        colunas_descartar = ['End Date', 'Serial Number', 'Updated At', 'Opened']
        for col in colunas_descartar:
            if col in df_user_product.columns:
                df_user_product = df_user_product.drop(columns=[col])
        
        if 'ID' in df_user_product.columns:
            df_user_product = df_user_product.rename(columns={'ID': 'Store Product ID'})
        
        if 'Created At' in df_user_product.columns:
            df_user_product = df_user_product.rename(columns={'Created At': 'Store Product Created At'})
        
        # 3. df_product
        colunas_descartar = ['Description', 'Cover Picture URL', 'Hash', 'Tags', 'Redeemable']
        for col in colunas_descartar:
            if col in df_product.columns:
                df_product = df_product.drop(columns=[col])
        
        if 'ID' in df_product.columns:
            df_product = df_product.rename(columns={'ID': 'Product ID'})
        
        # Extrair pontos da coluna Metadata
        def extrair_pontos_metadata(metadata_str):
            """Extrai pontos da string de metadata JSON com tratamento robusto."""
            try:
                if pd.isna(metadata_str) or metadata_str == '' or not isinstance(metadata_str, str):
                    return 0
                
                try:
                    metadata_dict = json.loads(metadata_str)
                except:
                    metadata_dict = json.loads(metadata_str.replace("'", '"'))
                
                if isinstance(metadata_dict, dict) and 'points' in metadata_dict:
                    return float(metadata_dict.get('points', 0))
                else:
                    return 0
            except Exception:
                return 0
        
        if 'Metadata' in df_product.columns:
            df_product['Product Points'] = df_product['Metadata'].apply(extrair_pontos_metadata)
        else:
            df_product['Product Points'] = 0
        
        # 4. df_boost_trans
        colunas_descartar = ['Hash']
        for col in colunas_descartar:
            if col in df_boost_trans.columns:
                df_boost_trans = df_boost_trans.drop(columns=[col])
        
        if 'ID' in df_boost_trans.columns:
            df_boost_trans = df_boost_trans.rename(columns={'ID': 'Boost Transaction ID'})
        
        # 5. df_boost
        colunas_descartar = ['Con Figs', 'Cover Picture URL', 'Description', 'Allow Points Purchase']
        for col in colunas_descartar:
            if col in df_boost.columns:
                df_boost = df_boost.drop(columns=[col])
        
        if 'ID' in df_boost.columns:
            df_boost = df_boost.rename(columns={'ID': 'Boost ID'})
        if 'Name' in df_boost.columns:
            df_boost = df_boost.rename(columns={'Name': 'Boost Name'})
        
        # 6. df_partner
        colunas_descartar = ['Logo URL', 'Settings', 'Discord Guild ID', 'Description', 
                           'Discord URL', 'Insta Gram URL', 'Modalities', 'Site URL', 'Twitch URL']
        for col in colunas_descartar:
            if col in df_partner.columns:
                df_partner = df_partner.drop(columns=[col])
        
        if 'ID' in df_partner.columns:
            df_partner = df_partner.rename(columns={'ID': 'Partner ID'})
        if 'Name' in df_partner.columns:
            df_partner = df_partner.rename(columns={'Name': 'Partner Name'})
        
        # 7. df_campaign
        colunas_descartar = ['Description', 'Cover Picture URL', 'Start Date', 'Finish Date', 
                           'Status', 'Highlight', 'Premium', 'Sponsored', 'Updated At']
        for col in colunas_descartar:
            if col in df_campaign.columns:
                df_campaign = df_campaign.drop(columns=[col])
        
        renames_campaign = {'ID': 'Campaign ID', 'Created At': 'Campaign Created At', 'Name': 'Campaign Name'}
        for old_name, new_name in renames_campaign.items():
            if old_name in df_campaign.columns:
                df_campaign = df_campaign.rename(columns={old_name: new_name})
        
        # 8. df_campaign_user
        colunas_descartar = ['Updated At']
        for col in colunas_descartar:
            if col in df_campaign_user.columns:
                df_campaign_user = df_campaign_user.drop(columns=[col])
        
        renames_campaign_user = {'ID': 'Campaign User ID', 'Created At': 'Campaign User Created At'}
        for old_name, new_name in renames_campaign_user.items():
            if old_name in df_campaign_user.columns:
                df_campaign_user = df_campaign_user.rename(columns={old_name: new_name})
        
        # 9. df_campaign_quest
        colunas_descartar = ['Updated At']
        for col in colunas_descartar:
            if col in df_campaign_quest.columns:
                df_campaign_quest = df_campaign_quest.drop(columns=[col])
        
        renames_campaign_quest = {'ID': 'Campaign Quest ID', 'Created At': 'Campaign Quest Created At'}
        for old_name, new_name in renames_campaign_quest.items():
            if old_name in df_campaign_quest.columns:
                df_campaign_quest = df_campaign_quest.rename(columns={old_name: new_name})
        
        # 10. df_reward
        if 'ID' in df_reward.columns:
            df_reward = df_reward.rename(columns={'ID': 'Reward ID'})
        
        # 11. df_subscription
        colunas_descartar = ['Updated At']
        for col in colunas_descartar:
            if col in df_subscription.columns:
                df_subscription = df_subscription.drop(columns=[col])
        
        if 'ID' in df_subscription.columns:
            df_subscription = df_subscription.rename(columns={'ID': 'Subscription ID'})
        
        if 'Created At' in df_subscription.columns:
            df_subscription['Subscription Created At'] = pd.to_datetime(df_subscription['Created At'], errors='coerce')
        
        if 'Start Date' in df_subscription.columns:
            df_subscription['Start Date'] = pd.to_datetime(df_subscription['Start Date'], errors='coerce')
        
        # 12. df_user
        colunas_descartar_user = ['Lottery Numbers', 'Pin', 'Full Name', 
                                'Banner Picture URL', 'Profile Picture URL', 'User Preferences']
        for col in colunas_descartar_user:
            if col in df_user.columns:
                df_user = df_user.drop(columns=[col])
        
        renames_user = {'Score': 'Actual Points', 'ID': 'User ID'}
        for old_name, new_name in renames_user.items():
            if old_name in df_user.columns:
                df_user = df_user.rename(columns={old_name: new_name})
        
        # Criar Faixa Etária
        if 'Birth Date' in df_user.columns:
            try:
                df_user['Birth Date'] = pd.to_datetime(df_user['Birth Date'], errors='coerce')
                current_year = datetime.now().year
                df_user['Age'] = current_year - df_user['Birth Date'].dt.year
                age_bins = [0, 18, 24, 34, 44, 54, 64, 100]
                age_labels = ['<18', '18-24', '25-34', '35-44', '45-54', '55-64', '65+']
                df_user['Faixa_Etaria'] = pd.cut(df_user['Age'], bins=age_bins, labels=age_labels, right=False)
            except Exception:
                df_user['Faixa_Etaria'] = 'Não informado'
        
        if 'Created At' in df_user.columns:
            df_user['User Created At'] = pd.to_datetime(df_user['Created At'], errors='coerce')
        
        # 13. df_user_partner_score
        colunas_descartar_ups = ['Updated At']
        for col in colunas_descartar_ups:
            if col in df_user_partner_score.columns:
                df_user_partner_score = df_user_partner_score.drop(columns=[col])
        
        renames_ups = {
            'Score': 'Partner Points', 
            'ID': 'User Partner Score ID', 
            'Created At': 'User Partner Score Created At'
        }
        for old_name, new_name in renames_ups.items():
            if old_name in df_user_partner_score.columns:
                df_user_partner_score = df_user_partner_score.rename(columns={old_name: new_name})
        
        return (df_transacoes, df_user_product, df_product, df_boost_trans, df_boost, df_partner,
                df_campaign, df_campaign_user, df_campaign_quest, df_reward, df_user, df_user_partner_score, df_subscription)
    
    except FileNotFoundError as e:
        st.error(f"Erro ao carregar arquivo: {e}")
        st.error("Certifique-se de que todos os arquivos CSV estão no diretório 'data_new/'.")
        st.stop()

def fazer_merge_campanhas_corrigido(df_campaign_user, df_campaign, df_reward, df_product, df_partner, df_user):
    """
    LÓGICA RIGOROSA COM INNER JOINS: Constrói DataFrame apenas com dados válidos e completos.
    """
    
    # Debug: Verificar dados de entrada
    if len(df_campaign_user) == 0:
        st.warning("DataFrame campaign_user está vazio")
        return pd.DataFrame()
    
    # PASSO 1: NORMALIZAR COLUNA STATUS E FILTRAR (CORREÇÃO CRÍTICA)
    if 'Status' in df_campaign_user.columns:
        # Normalizar para lowercase para resolver problema de case sensitivity
        df_campaign_user_copy = df_campaign_user.copy()
        df_campaign_user_copy['Status'] = df_campaign_user_copy['Status'].str.lower()
        
        # Verificar valores únicos após normalização para debug
        status_values = df_campaign_user_copy['Status'].unique()
        #st.info(f"Valores únicos de Status encontrados: {status_values}")
        
        # Filtrar por status 'completed' (lowercase)
        df_base = df_campaign_user_copy[df_campaign_user_copy['Status'] == 'completed'].copy()
        #st.info(f"Missões completadas encontradas: {len(df_base)}")
    else:
        df_base = df_campaign_user.copy()
        #st.info(f"Total de registros em campaign_user: {len(df_base)}")
    
    if len(df_base) == 0:
        st.warning("Nenhuma missão com status 'completed' encontrada")
        return pd.DataFrame()
    
    # PASSO 2: INNER JOIN com Campaign - OBRIGATÓRIO
    if 'Campaign ID' in df_base.columns and 'Campaign ID' in df_campaign.columns:
        before_count = len(df_base)
        df_base = pd.merge(df_base, df_campaign, on='Campaign ID', how='inner')
        #st.info(f"Após INNER JOIN com Campaign: {before_count} -> {len(df_base)} registros")
        
        if len(df_base) == 0:
            st.error("INNER JOIN com Campaign resultou em DataFrame vazio")
            return pd.DataFrame()
    else:
        st.error("Colunas Campaign ID não encontradas para merge")
        return pd.DataFrame()
    
    # PASSO 3: INNER JOIN com Reward - OBRIGATÓRIO  
    if 'Campaign ID' in df_base.columns and 'Campaign ID' in df_reward.columns:
        before_count = len(df_base)
        df_base = pd.merge(df_base, df_reward, on='Campaign ID', how='inner')
        
        if len(df_base) == 0:
            st.error("INNER JOIN com Reward resultou em DataFrame vazio")
            return pd.DataFrame()
    else:
        st.error("Colunas Campaign ID não encontradas para merge com Reward")
        return pd.DataFrame()
    
    # PASSO 4: INNER JOIN com Product - OBRIGATÓRIO para obter pontos
    if 'Product ID' in df_base.columns and 'Product ID' in df_product.columns:
        before_count = len(df_base)
        df_base = pd.merge(df_base, df_product[['Product ID', 'Product Points', 'Name', 'Type']], 
                          on='Product ID', how='inner', suffixes=('', '_product'))
        
        if len(df_base) == 0:
            st.error("INNER JOIN com Product resultou em DataFrame vazio")
            return pd.DataFrame()
            
        # Renomear colunas para evitar conflitos
        if 'Name' in df_base.columns:
            df_base = df_base.rename(columns={'Name': 'Product Name'})
    else:
        st.error("Colunas Product ID não encontradas para merge com Product")
        return pd.DataFrame()
    
    # PASSO 5: LEFT JOIN com Partner para obter nome do parceiro (secundário)
    if 'Partner ID' in df_base.columns and 'Partner ID' in df_partner.columns:
        before_count = len(df_base)
        df_base = pd.merge(df_base, df_partner, on='Partner ID', how='left')
    
    # PASSO 6: LEFT JOIN com User para obter dados demográficos (secundário)
    if 'User ID' in df_base.columns and 'User ID' in df_user.columns:
        user_cols = ['User ID', 'Username', 'Email', 'Actual Points', 'Faixa_Etaria']
        if 'Age' in df_user.columns:
            user_cols.append('Age')
        before_count = len(df_base)
        
        # Verificar se Email já existe para evitar duplicação
        suffixes = ('', '_user') if 'Email' in df_base.columns else ('', '')
        df_base = pd.merge(df_base, df_user[user_cols], on='User ID', how='left', suffixes=suffixes)
    
    # PASSO 7: Adicionar colunas de data processadas
    if 'Campaign User Created At' in df_base.columns:
        try:
            df_base['Campaign User Created At'] = pd.to_datetime(df_base['Campaign User Created At'], errors='coerce')
            df_base['data_participacao'] = df_base['Campaign User Created At'].dt.date
            df_base['semana_participacao'] = df_base['Campaign User Created At'].dt.to_period('W')
        except Exception as e:
            st.warning(f"Erro ao processar datas: {e}")
    
    # Verificar se temos pontos válidos
    if 'Product Points' in df_base.columns:
        total_pontos = df_base['Product Points'].sum()
    else:
        st.error("Coluna Product Points não encontrada no resultado final")
    
    return df_base

def fazer_merge_rewards_corrigido(df_transacoes, df_user_product, df_product, df_partner, df_user):
    """
    LÓGICA CORRIGIDA: Merge de recompensas evitando duplicação e conflitos de coluna.
    """
    
    # PASSO 1: Merge inicial por User ID
    if 'User ID' in df_transacoes.columns and 'User ID' in df_user_product.columns:
        df_merged = pd.merge(df_transacoes, df_user_product, on='User ID', how='inner')
    else:
        return pd.DataFrame()
    
    # PASSO 2: Juntar com Product
    if 'Product ID' in df_merged.columns and 'Product ID' in df_product.columns:
        df_merged = pd.merge(df_merged, df_product, on='Product ID', how='left')
    
    # PASSO 3: Juntar com Partner
    if 'Partner ID' in df_merged.columns and 'Partner ID' in df_partner.columns:
        df_merged = pd.merge(df_merged, df_partner, on='Partner ID', how='left')
    
    # PASSO 4: Juntar com User (GERENCIAMENTO INTELIGENTE DE EMAIL)
    if 'User ID' in df_merged.columns and 'User ID' in df_user.columns:
        user_cols = ['User ID', 'Username', 'Actual Points', 'Faixa_Etaria']
        
        # Adicionar Email apenas se não existir para evitar conflitos
        if 'Email' not in df_merged.columns:
            user_cols.append('Email')
        
        if 'Age' in df_user.columns:
            user_cols.append('Age')
            
        df_final = pd.merge(df_merged, df_user[user_cols], on='User ID', how='left')
    else:
        df_final = df_merged
    
    return df_final

def fazer_merge_boosts_corrigido(df_subscription, df_boost, df_partner, df_user):
    """
    LÓGICA CORRIGIDA: Merge de boosts baseado em subscriptions.
    """
    
    # PASSO 1: Começar com subscription como base
    df_merged = pd.merge(df_subscription, df_boost, on='Boost ID', how='left')
    
    # PASSO 2: Juntar com Partner
    if 'Partner ID' in df_merged.columns and 'Partner ID' in df_partner.columns:
        df_merged = pd.merge(df_merged, df_partner, on='Partner ID', how='left')
    
    # PASSO 3: Juntar com User
    if 'User ID' in df_merged.columns and 'User ID' in df_user.columns:
        user_cols = ['User ID', 'Username', 'Actual Points', 'Faixa_Etaria']
        if 'Age' in df_user.columns:
            user_cols.append('Age')
        df_final = pd.merge(df_merged, df_user[user_cols], on='User ID', how='left')
    else:
        df_final = df_merged
    
    # PASSO 4: Processamento de datas
    if 'Subscription Created At' in df_final.columns:
        df_final['semana_boost'] = df_final['Subscription Created At'].dt.to_period('W')
        df_final['data_boost'] = df_final['Subscription Created At'].dt.date
        df_final['dia_semana'] = df_final['Subscription Created At'].dt.day_name()
        
        dias_pt = {
            'Monday': 'Segunda-feira', 'Tuesday': 'Terça-feira', 
            'Wednesday': 'Quarta-feira', 'Thursday': 'Quinta-feira',
            'Friday': 'Sexta-feira', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
        }
        df_final['dia_semana'] = df_final['dia_semana'].map(dias_pt)
        df_final['data_transacao'] = df_final['Subscription Created At'].dt.date
    
    return df_final

# ==================== FUNÇÕES PARA DASHBOARD GERAL ====================

def calcular_usuarios_engajados(df_rewards, df_boosts, df_campanhas, parceiro_selecionado):
    """Calcula usuários engajados seguindo a lógica especificada."""
    if parceiro_selecionado != "Todos os Parceiros":
        df_rewards_filt = df_rewards[df_rewards['Partner Name'] == parceiro_selecionado] if 'Partner Name' in df_rewards.columns else pd.DataFrame()
        df_boosts_filt = df_boosts[df_boosts['Partner Name'] == parceiro_selecionado] if 'Partner Name' in df_boosts.columns else pd.DataFrame()
        df_campanhas_filt = df_campanhas[df_campanhas['Partner Name'] == parceiro_selecionado] if 'Partner Name' in df_campanhas.columns else pd.DataFrame()
    else:
        df_rewards_filt = df_rewards
        df_boosts_filt = df_boosts
        df_campanhas_filt = df_campanhas
    
    usuarios_rewards = set(df_rewards_filt['User ID'].dropna().unique()) if 'User ID' in df_rewards_filt.columns and len(df_rewards_filt) > 0 else set()
    usuarios_boosts = set(df_boosts_filt['User ID'].dropna().unique()) if 'User ID' in df_boosts_filt.columns and len(df_boosts_filt) > 0 else set()
    usuarios_campanhas = set(df_campanhas_filt['User ID'].dropna().unique()) if 'User ID' in df_campanhas_filt.columns and len(df_campanhas_filt) > 0 else set()
    
    usuarios_engajados_set = usuarios_rewards | usuarios_boosts | usuarios_campanhas
    
    return usuarios_engajados_set

def calcular_total_pontos_gerados(df_campanhas, df_rewards, parceiro_selecionado):
    """
    FUNÇÃO CORRIGIDA: Calcula total de pontos usando Product Points das missões completadas.
    """
    if parceiro_selecionado != "Todos os Parceiros":
        df_campanhas_filt = df_campanhas[df_campanhas['Partner Name'] == parceiro_selecionado] if 'Partner Name' in df_campanhas.columns else df_campanhas
        df_rewards_filt = df_rewards[df_rewards['Partner Name'] == parceiro_selecionado] if 'Partner Name' in df_rewards.columns else df_rewards
    else:
        df_campanhas_filt = df_campanhas
        df_rewards_filt = df_rewards
    
    # Pontos de missões (Product Points das missões completadas)
    pontos_missoes = 0
    if len(df_campanhas_filt) > 0 and 'Product Points' in df_campanhas_filt.columns:
        pontos_missoes = df_campanhas_filt['Product Points'].sum()
    
    # Pontos de recompensas (Price das transações)
    pontos_recompensas = 0
    if len(df_rewards_filt) > 0 and 'Price' in df_rewards_filt.columns:
        pontos_recompensas = df_rewards_filt['Price'].sum()
    
    total_pontos = pontos_missoes + pontos_recompensas
    return total_pontos, pontos_missoes, pontos_recompensas

def calcular_kpis_dashboard_geral(df_rewards, df_boosts, df_campanhas, parceiro_selecionado):
    """
    FUNÇÃO CORRIGIDA: Calcula KPIs usando a lógica corrigida.
    """
    
    usuarios_engajados_set = calcular_usuarios_engajados(df_rewards, df_boosts, df_campanhas, parceiro_selecionado)
    usuarios_engajados = len(usuarios_engajados_set)
    
    if parceiro_selecionado != "Todos os Parceiros":
        df_rewards_filt = df_rewards[df_rewards['Partner Name'] == parceiro_selecionado] if 'Partner Name' in df_rewards.columns else df_rewards
        df_boosts_filt = df_boosts[df_boosts['Partner Name'] == parceiro_selecionado] if 'Partner Name' in df_boosts.columns else df_boosts
        df_campanhas_filt = df_campanhas[df_campanhas['Partner Name'] == parceiro_selecionado] if 'Partner Name' in df_campanhas.columns else df_campanhas
    else:
        df_rewards_filt = df_rewards
        df_boosts_filt = df_boosts
        df_campanhas_filt = df_campanhas
    
    # Pontos de missões usando Product Points
    pontos_missoes = 0
    if len(df_campanhas_filt) > 0 and 'Product Points' in df_campanhas_filt.columns:
        pontos_missoes = df_campanhas_filt['Product Points'].sum()
    
    recompensas_resgatadas = len(df_rewards_filt)
    novas_assinaturas = len(df_boosts_filt)
    
    total_pontos, _, _ = calcular_total_pontos_gerados(df_campanhas, df_rewards, parceiro_selecionado)
    
    return usuarios_engajados, pontos_missoes, recompensas_resgatadas, novas_assinaturas, total_pontos

# ==================== FUNÇÕES DE GRÁFICOS ATUALIZADAS ====================

def criar_grafico_novos_usuarios_por_semana(df_boosts):
    """Cria gráfico de usuários únicos com novas assinaturas por semana."""
    if 'Start Date' not in df_boosts.columns or 'User ID' not in df_boosts.columns:
        return None
    
    df_filtrado = df_boosts.copy()
    df_filtrado['Start Date'] = pd.to_datetime(df_filtrado['Start Date'], errors='coerce')
    df_filtrado = df_filtrado.dropna(subset=['Start Date', 'User ID'])
    
    if len(df_filtrado) == 0:
        return None
    
    data_limite = datetime.now() - timedelta(days=30)
    df_filtrado = df_filtrado[df_filtrado['Start Date'] >= data_limite]
    
    if len(df_filtrado) == 0:
        return None
    
    df_filtrado['semana'] = df_filtrado['Start Date'].dt.to_period('W')
    usuarios_por_semana = df_filtrado.groupby('semana')['User ID'].nunique().reset_index()
    usuarios_por_semana['semana_str'] = usuarios_por_semana['semana'].astype(str)
    
    if len(usuarios_por_semana) == 0:
        return None
    
    fig = px.bar(
        usuarios_por_semana,
        x='semana_str',
        y='User ID',
        title='Usuários semanais Únicos com Novas Assinaturas (Último Mês)',
        labels={'semana_str': 'Semana', 'User ID': 'Usuários Únicos'},
        color='User ID',
        color_continuous_scale='viridis'
    )
    
    fig.update_layout(height=400, title_x=0.5, xaxis_tickangle=-45, showlegend=False)
    
    return fig

def criar_grafico_total_assinaturas_por_boost(df_boosts):
    """Cria gráfico de barras com total de assinaturas por tipo de boost."""
    if 'Boost Name' not in df_boosts.columns:
        return None
    
    df_filtrado = df_boosts.dropna(subset=['Boost Name'])
    
    if len(df_filtrado) == 0:
        return None
    
    assinaturas_por_boost = df_filtrado['Boost Name'].value_counts()
    
    if len(assinaturas_por_boost) == 0:
        return None
    
    fig = px.bar(
        x=assinaturas_por_boost.index,
        y=assinaturas_por_boost.values,
        title='Total de Assinaturas por Tipo de Boost',
        labels={'x': 'Tipo de Boost', 'y': 'Total de Assinaturas'},
        color=assinaturas_por_boost.values,
        color_continuous_scale='viridis'
    )
    
    fig.update_layout(height=400, showlegend=False, title_x=0.5, xaxis_tickangle=-45)
    
    return fig

def criar_grafico_campanhas_pontos_tempo(df_campanhas):
    """Cria gráfico de Product Points ao longo do tempo."""
    if 'Product Points' not in df_campanhas.columns or 'semana_participacao' not in df_campanhas.columns:
        return None
    
    pontos_semanais = df_campanhas.groupby('semana_participacao')['Product Points'].sum().reset_index()
    pontos_semanais['semana_str'] = pontos_semanais['semana_participacao'].astype(str)
    
    if len(pontos_semanais) == 0:
        return None
    
    fig = px.line(
        pontos_semanais,
        x='semana_str',
        y='Product Points',
        title='Pontos de Missões Gerados ao Longo do Tempo',
        labels={'semana_str': 'Semana', 'Product Points': 'Pontos de Missões'},
        markers=True
    )
    
    fig.update_traces(line_color='#FF6B6B', line_width=3, marker_size=8)
    fig.update_layout(height=400, title_x=0.5, xaxis_tickangle=-45)
    
    return fig

def criar_grafico_top5_campanhas_engajamento(df_campanhas):
    """Cria gráfico das Top 5 campanhas por engajamento."""
    if 'Campaign Name' not in df_campanhas.columns:
        return None
    
    campanhas_count = df_campanhas['Campaign Name'].value_counts().head(5)
    
    if len(campanhas_count) == 0:
        return None
    
    fig = px.bar(
        x=campanhas_count.index,
        y=campanhas_count.values,
        title='Top 5 Campanhas por Engajamento',
        labels={'x': 'Campanha', 'y': 'Número de Participações'},
        color=campanhas_count.values,
        color_continuous_scale='viridis'
    )
    
    fig.update_layout(height=400, showlegend=False, title_x=0.5, xaxis_tickangle=-45)
    
    return fig

def criar_tabela_top_usuario(df_user, df_campanhas):
    """Cria tabela com informações do top usuário por Product Points."""
    if 'Username' not in df_campanhas.columns or 'Product Points' not in df_campanhas.columns:
        return None
    
    pontos_por_usuario = df_campanhas.groupby('Username')['Product Points'].sum()
    if len(pontos_por_usuario) == 0:
        return None
    
    top_usuario_nome = pontos_por_usuario.idxmax()
    total_product_points = pontos_por_usuario[top_usuario_nome]
    
    usuario_data = df_user[df_user['Username'] == top_usuario_nome]
    if usuario_data.empty:
        return None
    
    usuario = usuario_data.iloc[0]
    
    info_dict = {
        'Métrica': ['Username', 'Pontos de Missões Total'],
        'Valor': [top_usuario_nome, f"{total_product_points:,.0f}"]
    }
    
    if 'User ID' in usuario.index:
        info_dict['Métrica'].append('User ID')
        info_dict['Valor'].append(usuario['User ID'])
    
    if 'Actual Points' in usuario.index:
        info_dict['Métrica'].append('Saldo Atual')
        info_dict['Valor'].append(f"{usuario['Actual Points']:,.0f}")
    
    if 'Faixa_Etaria' in usuario.index:
        info_dict['Métrica'].append('Faixa Etária')
        info_dict['Valor'].append(str(usuario['Faixa_Etaria']))
    
    return pd.DataFrame(info_dict)

# ==================== NOVAS FUNÇÕES PARA ANÁLISE DE USUÁRIO ====================

def criar_grafico_distribuicao_faixa_etaria(df_user):
    """Cria gráfico de pizza com distribuição por faixa etária."""
    if 'Faixa_Etaria' not in df_user.columns:
        return None
    
    faixa_count = df_user['Faixa_Etaria'].value_counts()
    
    if len(faixa_count) == 0:
        return None
    
    fig = px.pie(
        values=faixa_count.values,
        names=faixa_count.index,
        title='Distribuição de Usuários por Faixa Etária',
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    fig.update_traces(textposition='inside', textinfo='percent+label')
    fig.update_layout(height=450, title_x=0.5)
    
    return fig

def criar_grafico_top10_usuarios_product_points(df_campanhas):
    """Cria gráfico de barras com top 10 usuários por Product Points."""
    if 'Username' not in df_campanhas.columns or 'Product Points' not in df_campanhas.columns:
        return None
    
    pontos_por_usuario = df_campanhas.groupby('Username')['Product Points'].sum().nlargest(10)
    
    if len(pontos_por_usuario) == 0:
        return None
    
    top_usuario = pontos_por_usuario.idxmax() if len(pontos_por_usuario) > 0 else None
    
    colors = ['#FFD700' if username == top_usuario else '#4ECDC4' for username in pontos_por_usuario.index]
    
    fig = px.bar(
        x=pontos_por_usuario.index,
        y=pontos_por_usuario.values,
        title='Top 10 Usuários por Pontos de Missões',
        labels={'x': 'Usuário', 'y': 'Product Points Total'},
    )
    
    fig.update_traces(marker_color=colors)
    fig.update_layout(height=500, showlegend=False, title_x=0.5, xaxis_tickangle=-45)
    
    return fig

# ==================== FUNÇÕES RESTAURADAS PARA ANÁLISE DE REWARDS ====================

def criar_grafico_pontos_resgatados_item(df_rewards):
    """Cria gráfico de total de pontos resgatados por item."""
    nome_col = None
    for col in ['Name', 'Product Name']:
        if col in df_rewards.columns:
            nome_col = col
            break
    
    if not nome_col or 'Price' not in df_rewards.columns:
        return None
    
    # Filtrar apenas registros válidos
    df_filtrado = df_rewards.dropna(subset=[nome_col, 'Price'])
    
    if len(df_filtrado) == 0:
        return None
    
    pontos_por_item = df_filtrado.groupby(nome_col)['Price'].sum().sort_values(ascending=False).head(10)
    
    if pontos_por_item.sum() == 0:
        return None
    
    fig = px.bar(
        x=pontos_por_item.index,
        y=pontos_por_item.values,
        title='Total de Pontos Resgatados por Item',
        labels={'x': 'Item', 'y': 'Total de Pontos'},
        color=pontos_por_item.values,
        color_continuous_scale='viridis'
    )
    
    fig.update_layout(height=500, showlegend=False, title_x=0.5, xaxis_tickangle=-45)
    
    return fig

def criar_grafico_unidades_resgatadas_item(df_rewards):
    """Cria gráfico de total de unidades resgatadas por item."""
    nome_col = None
    for col in ['Name', 'Product Name']:
        if col in df_rewards.columns:
            nome_col = col
            break
    
    if not nome_col:
        return None
    
    unidades_por_item = df_rewards[nome_col].value_counts()#.head(10)
    
    if len(unidades_por_item) == 0:
        return None
    
    fig = px.bar(
        x=unidades_por_item.index,
        y=unidades_por_item.values,
        title='Total de Unidades Resgatadas por Item',
        labels={'x': 'Item', 'y': 'Quantidade Resgatada'},
        color=unidades_por_item.values,
        color_continuous_scale='viridis'
    )
    
    fig.update_layout(height=500, showlegend=False, title_x=0.5, xaxis_tickangle=-45)
    
    return fig

# ==================== FUNÇÕES RESTAURADAS PARA ANÁLISE DE CAMPANHAS ====================

def criar_grafico_participacoes_tempo(df_campanhas):
    """Cria gráfico de série temporal de participações em campanhas."""
    if 'data_participacao' not in df_campanhas.columns:
        return None
    
    participacoes_diarias = df_campanhas.groupby('data_participacao').size().reset_index(name='participacoes')
    participacoes_diarias = participacoes_diarias.sort_values('data_participacao')
    
    if len(participacoes_diarias) == 0:
        return None
    
    fig = px.line(
        participacoes_diarias,
        x='data_participacao',
        y='participacoes',
        title='Participações em Campanhas ao Longo do Tempo',
        labels={'data_participacao': 'Data', 'participacoes': 'Número de Participações'}
    )
    
    fig.update_traces(line_color='#4ECDC4', line_width=3)
    fig.update_layout(height=400, title_x=0.5)
    
    return fig

def criar_grafico_engajamento_dia_semana(df_campanhas):
    """Cria gráfico de engajamento por dia da semana."""
    if 'Campaign User Created At' not in df_campanhas.columns:
        return None
    
    try:
        df_temp = df_campanhas.copy()
        df_temp['dia_semana_camp'] = pd.to_datetime(df_temp['Campaign User Created At']).dt.day_name()
        
        dias_pt = {
            'Monday': 'Segunda-feira', 'Tuesday': 'Terça-feira', 'Wednesday': 'Quarta-feira',
            'Thursday': 'Quinta-feira', 'Friday': 'Sexta-feira', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
        }
        df_temp['dia_semana_camp'] = df_temp['dia_semana_camp'].map(dias_pt)
        
        ordem_dias = ['Segunda-feira', 'Terça-feira', 'Quarta-feira', 'Quinta-feira', 
                      'Sexta-feira', 'Sábado', 'Domingo']
        
        participacoes_por_dia = df_temp['dia_semana_camp'].value_counts().reindex(ordem_dias, fill_value=0)
        
        fig = px.bar(
            x=participacoes_por_dia.index,
            y=participacoes_por_dia.values,
            title='Engajamento em Campanhas por Dia da Semana',
            labels={'x': 'Dia da Semana', 'y': 'Número de Participações'},
            color=participacoes_por_dia.values,
            color_continuous_scale='viridis'
        )
        
        fig.update_layout(height=400, showlegend=False, title_x=0.5, xaxis_tickangle=-45)
        
        return fig
    except Exception:
        return None

def criar_grafico_engajamento_por_hora(df_campanhas):
    """Cria gráfico de engajamento por hora do dia."""
    if 'Campaign User Created At' not in df_campanhas.columns:
        return None
    
    try:
        df_temp = df_campanhas.copy()
        df_temp['hora_participacao'] = pd.to_datetime(df_temp['Campaign User Created At']).dt.hour
        
        participacoes_por_hora = df_temp['hora_participacao'].value_counts().sort_index()
        
        fig = px.bar(
            x=participacoes_por_hora.index,
            y=participacoes_por_hora.values,
            title='Engajamento em Campanhas por Hora do Dia',
            labels={'x': 'Hora do Dia (0-23)', 'y': 'Número de Participações'},
            color=participacoes_por_hora.values,
            color_continuous_scale='viridis'
        )
        
        fig.update_layout(height=400, showlegend=False, title_x=0.5)
        fig.update_xaxes(tickmode='linear', tick0=0, dtick=2)
        
        return fig
    except Exception:
        return None

def main():
    """Função principal que executa toda a aplicação Streamlit."""
    st.set_page_config(
        page_title="Dashboard de Análise - Parceiro W7M",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    st.title("📊 Dashboard de Análise - Parceiro W7M")
    st.markdown("---")
    
    # Carregar dados
    @st.cache_data
    def load_and_merge_all_data():
        (df_transacoes, df_user_product, df_product, df_boost_trans, df_boost, df_partner,
         df_campaign, df_campaign_user, df_campaign_quest, df_reward, df_user, df_user_partner_score, df_subscription) = carregar_dados()
        
        # Usar as funções corrigidas de merge
        df_rewards = fazer_merge_rewards_corrigido(df_transacoes, df_user_product, df_product, df_partner, df_user)
        df_boosts = fazer_merge_boosts_corrigido(df_subscription, df_boost, df_partner, df_user)
        df_campanhas = fazer_merge_campanhas_corrigido(df_campaign_user, df_campaign, df_reward, df_product, df_partner, df_user)
        
        return df_rewards, df_boosts, df_campanhas, df_partner, df_user, df_product
    
    # Carregar dados
    with st.spinner('Carregando dados do parceiro W7M...'):
        df_rewards, df_boosts, df_campanhas, df_partner, df_user, df_product = load_and_merge_all_data()
    
    # Filtrar para W7M
    partner_name = 'W7M'
    df_rewards_w7m = df_rewards[df_rewards['Partner Name'] == partner_name].copy() if 'Partner Name' in df_rewards.columns else pd.DataFrame()
    df_boosts_w7m = df_boosts[df_boosts['Partner Name'] == partner_name].copy() if 'Partner Name' in df_boosts.columns else pd.DataFrame()
    df_campanhas_w7m = df_campanhas[df_campanhas['Partner Name'] == partner_name].copy() if 'Partner Name' in df_campanhas.columns else pd.DataFrame()
    
    # Criar abas
    tab_dashboard, tab_usuario, tab_rewards, tab_boosts, tab_campaigns = st.tabs([
        "🏠 Dashboard Geral W7M",
        "👤 Análise de Usuário W7M",
        "🎁 Análise de Rewards W7M", 
        "🚀 Análise de Boosts W7M", 
        "🎯 Análise de Campanhas W7M"
    ])
    
    # ==================== DASHBOARD GERAL W7M ====================
    with tab_dashboard:
        st.header("🏠 Dashboard Geral - W7M")
        st.caption("Visão executiva do parceiro W7M")
        
        # KPIs Dinâmicos
        parceiro_selecionado = 'W7M'
        usuarios_engajados, pontos_missoes, recompensas_resgatadas, novas_assinaturas, total_pontos = \
            calcular_kpis_dashboard_geral(df_rewards_w7m, df_boosts_w7m, df_campanhas_w7m, parceiro_selecionado)
        
        # KPIs para W7M
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("👥 Usuários Engajados W7M", f"{usuarios_engajados:,}")
        with col2:
            st.metric("💎 Pontos de Missões W7M", f"{pontos_missoes:,.0f}")
        with col3:
            st.metric("🎁 Recompensas Resgatadas", f"{recompensas_resgatadas:,}")
        with col4:
            st.metric("🚀 Assinaturas de Boost", f"{novas_assinaturas:,}")
        with col5:
            st.metric("⭐ Total de Pontos Gerados", f"{total_pontos:,.0f}")
        
        st.markdown("---")
        
        # Gráficos principais
        col1, col2 = st.columns(2)
        
        with col1:
            fig_pontos_tempo = criar_grafico_campanhas_pontos_tempo(df_campanhas_w7m)
            if fig_pontos_tempo:
                st.plotly_chart(fig_pontos_tempo, use_container_width=True)
            else:
                st.info("Dados de pontos de missões não disponíveis")
        
        with col2:
            fig_top_campanhas = criar_grafico_top5_campanhas_engajamento(df_campanhas_w7m)
            if fig_top_campanhas:
                st.plotly_chart(fig_top_campanhas, use_container_width=True)
            else:
                st.info("Dados de campanhas não disponíveis")
        
        # Gráficos de boosts
        col1, col2 = st.columns(2)
        
        with col1:
            fig_novos_usuarios = criar_grafico_novos_usuarios_por_semana(df_boosts_w7m)
            if fig_novos_usuarios:
                st.plotly_chart(fig_novos_usuarios, use_container_width=True)
            else:
                st.info("Dados de novos usuários não disponíveis")
        
        with col2:
            fig_total_boosts = criar_grafico_total_assinaturas_por_boost(df_boosts_w7m)
            if fig_total_boosts:
                st.plotly_chart(fig_total_boosts, use_container_width=True)
            else:
                st.info("Dados de boosts não disponíveis")
        
        # Visualização dos dados
        with st.expander("Visualizar Dados Brutos de Campanhas"):
            st.dataframe(df_campanhas_w7m)
    
    # ==================== ANÁLISE DE USUÁRIO W7M APRIMORADA ====================
    with tab_usuario:
        st.header("👤 Análise de Usuário W7M")
        st.caption("Perfil e comportamento dos usuários do parceiro W7M")
        
        # Calcular usuários engajados
        usuarios_engajados_set = calcular_usuarios_engajados(df_rewards_w7m, df_boosts_w7m, df_campanhas_w7m, parceiro_selecionado)
        
        # Métricas
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_usuarios = len(usuarios_engajados_set)
            st.metric("Total de Usuários W7M", f"{total_usuarios:,}")
        
        with col2:
            if 'Product Points' in df_campanhas_w7m.columns and len(df_campanhas_w7m) > 0:
                media_pontos = df_campanhas_w7m['Product Points'].mean()
                st.metric("Média Pontos Missão/Usuário", f"{media_pontos:,.1f}")
            else:
                st.metric("Média Pontos Missão/Usuário", "0")
        
        with col3:
            if 'Actual Points' in df_user.columns and len(usuarios_engajados_set) > 0:
                usuarios_ativos = len(df_user[(df_user['User ID'].isin(usuarios_engajados_set)) & (df_user['Actual Points'] > 0)])
                st.metric("Usuários Ativos W7M", f"{usuarios_ativos:,}")
            else:
                st.metric("Usuários Ativos W7M", "0")
        
        st.markdown("---")
        
        # NOVOS GRÁFICOS DE ANÁLISE DE USUÁRIO
        col1, col2 = st.columns(2)
        
        with col1:
            fig_faixa_etaria = criar_grafico_distribuicao_faixa_etaria(df_user)
            if fig_faixa_etaria:
                st.plotly_chart(fig_faixa_etaria, use_container_width=True)
            else:
                st.info("Dados de faixa etária não disponíveis")
        
        with col2:
            fig_top_usuarios = criar_grafico_top10_usuarios_product_points(df_campanhas_w7m)
            if fig_top_usuarios:
                st.plotly_chart(fig_top_usuarios, use_container_width=True)
            else:
                st.info("Dados de usuários por pontos de missão não disponíveis")
        
        # Tabela do Top Usuário
        st.markdown("---")
        st.subheader("🌟 Destaque W7M: Top Usuário por Pontos de Missões")
        
        tabela_top = criar_tabela_top_usuario(df_user, df_campanhas_w7m)
        if tabela_top is not None:
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                st.dataframe(tabela_top, use_container_width=True, hide_index=True)
        else:
            st.info("Dados do top usuário não disponíveis para W7M")
        
        with st.expander("Visualizar Dados Brutos de Campanhas"):
            st.dataframe(df_campanhas_w7m)
    
    # ==================== ANÁLISE DE REWARDS W7M ====================
    with tab_rewards:
        st.header("🎁 Análise de Rewards W7M")
        st.caption("Análise detalhada de recompensas resgatadas no parceiro W7M")
        
        # KPIs
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_transacoes = len(df_rewards_w7m)
            st.metric("Total de Transações W7M", f"{total_transacoes:,}")
        
        with col2:
            usuarios_unicos = df_rewards_w7m['User ID'].nunique() if 'User ID' in df_rewards_w7m.columns else 0
            st.metric("Usuários Únicos W7M", f"{usuarios_unicos:,}")
        
        with col3:
            if 'Transaction ID' in df_rewards_w7m.columns:
                df_rewards_clean = df_rewards_w7m.drop_duplicates(subset=['Transaction ID'])
            else:
                df_rewards_clean = df_rewards_w7m
                
            total_pontos_rewards = df_rewards_clean['Price'].sum() if 'Price' in df_rewards_clean.columns else 0
            st.metric("Total de Pontos Resgatados", f"{total_pontos_rewards:,.0f}")
        
        st.markdown("---")
        
        # GRÁFICOS RESTAURADOS
        col1, col2 = st.columns(2)
        
        with col1:
            fig_pontos = criar_grafico_pontos_resgatados_item(df_rewards_w7m)
            if fig_pontos:
                st.plotly_chart(fig_pontos, use_container_width=True)
            else:
                st.info("Dados de pontos por item não disponíveis")
        
        with col2:
            fig_unidades = criar_grafico_unidades_resgatadas_item(df_rewards_w7m)
            if fig_unidades:
                st.plotly_chart(fig_unidades, use_container_width=True)
            else:
                st.info("Dados de unidades por item não disponíveis")
        
        # Lista detalhada
        st.markdown("---")
        st.subheader("📋 Detalhamento de Resgates por Usuário")
        
        if not df_rewards_w7m.empty:
            # CORREÇÃO: Gerenciamento inteligente da coluna Email
            email_col = 'Email'
            
            # Se Email não existe nos rewards, fazer merge com user
            if 'Email' not in df_rewards_w7m.columns:
                df_com_email = pd.merge(
                    df_rewards_w7m,
                    df_user[['User ID', 'Email']],
                    on='User ID',
                    how='left'
                )
            else:
                # Email já existe, usar diretamente
                df_com_email = df_rewards_w7m.copy()
                
            #st.write('df_com_email', df_com_email)
            
            # Remover duplicatas por Transaction ID se existir
            if 'Transaction ID' in df_com_email.columns:
                df_com_email = df_com_email.drop_duplicates(subset=['Store Product ID_y'])
            
            # Criar resumo por usuário e produto
            #st.write('df_com_email 2', df_com_email)
            if 'Name' in df_com_email.columns and 'Username' in df_com_email.columns and email_col in df_com_email.columns:
                resumo_resgates = df_com_email.groupby(['Username', email_col, 'Name']).size().reset_index(name='Quantidade')
                st.dataframe(resumo_resgates)
            else:
                st.info("Dados de resgates não disponíveis")
        else:
            st.info("Não há dados de resgate para exibir.")
        
        with st.expander("Visualizar Dados Brutos de Rewards"):
            st.dataframe(df_rewards_w7m)
    
    # ==================== ANÁLISE DE BOOSTS W7M ====================
    with tab_boosts:
        st.header("🚀 Análise de Boosts W7M")
        st.caption("Análise detalhada de assinaturas de boost do parceiro W7M")
        
        # KPIs
        col1, col2 = st.columns(2)
        
        with col1:
            total_assinaturas = len(df_boosts_w7m)
            st.metric("📊 Total de Assinaturas W7M", f"{total_assinaturas:,}")
        
        with col2:
            usuarios_unicos = df_boosts_w7m['User ID'].nunique() if 'User ID' in df_boosts_w7m.columns else 0
            st.metric("👥 Usuários Únicos W7M", f"{usuarios_unicos:,}")
        
        with st.expander("Visualizar Dados Brutos de Boosts"):
            st.dataframe(df_boosts_w7m)
    
    # ==================== ANÁLISE DE CAMPANHAS W7M ====================
    with tab_campaigns:
        st.header("🎯 Análise de Campanhas W7M")
        st.caption("Análise detalhada de engajamento em campanhas do parceiro W7M")
        
        # KPIs
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_participacoes = len(df_campanhas_w7m)
            st.metric("Total de Participações W7M", f"{total_participacoes:,}")
        
        with col2:
            usuarios_unicos = df_campanhas_w7m['User ID'].nunique() if 'User ID' in df_campanhas_w7m.columns else 0
            st.metric("Usuários Únicos W7M", f"{usuarios_unicos:,}")
        
        with col3:
            total_pontos_missoes = df_campanhas_w7m['Product Points'].sum() if 'Product Points' in df_campanhas_w7m.columns else 0
            st.metric("Pontos de Missões Total W7M", f"{total_pontos_missoes:,.0f}")
        
        st.markdown("---")
        
        # ANÁLISES TEMPORAIS RESTAURADAS
        fig_tempo = criar_grafico_participacoes_tempo(df_campanhas_w7m)
        if fig_tempo:
            st.plotly_chart(fig_tempo, use_container_width=True)
        else:
            st.info("Dados de série temporal não disponíveis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_dia_semana = criar_grafico_engajamento_dia_semana(df_campanhas_w7m)
            if fig_dia_semana:
                st.plotly_chart(fig_dia_semana, use_container_width=True)
            else:
                st.info("Dados de engajamento por dia não disponíveis")
        
        with col2:
            fig_por_hora = criar_grafico_engajamento_por_hora(df_campanhas_w7m)
            if fig_por_hora:
                st.plotly_chart(fig_por_hora, use_container_width=True)
            else:
                st.info("Dados de engajamento por hora não disponíveis")
        
        with st.expander("Visualizar Dados Brutos de Campanhas"):
            st.dataframe(df_campanhas_w7m)

if __name__ == "__main__":
    main()











