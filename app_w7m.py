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
    Aplica limpeza e renomeação de colunas logo no início.
    
    Returns:
        tuple: DataFrames limpos para análises de rewards, boosts e campanhas
    """
    try:
        # ==================== CARREGAMENTO DOS DADOS ORIGINAIS ====================
        df_transacoes = pd.read_csv('data_new/store_transaction.csv')
        df_product = pd.read_csv('data_new/product.csv')
        df_boost_trans = pd.read_csv('data_new/boost_transaction.csv')
        df_boost = pd.read_csv('data_new/boost.csv')
        df_partner = pd.read_csv('data_new/partner.csv')
        
        # ==================== CARREGAMENTO DOS NOVOS DADOS ====================
        df_user_product = pd.read_csv('data_new/user_product.csv')
        df_campaign = pd.read_csv('data_new/campaign.csv')
        df_campaign_user = pd.read_csv('data_new/campaign_user.csv')
        df_campaign_quest = pd.read_csv('data_new/campaign_quest.csv')
        df_reward = pd.read_csv('data_new/reward.csv')
        df_subscription = pd.read_csv('data_new/subscription.csv')
        
        # ==================== CARREGAMENTO DAS TABELAS DE USUÁRIO ====================
        df_user = pd.read_csv('data_new/user.csv')
        df_user_partner_score = pd.read_csv('data_new/user_partner_score.csv')
        
        # ==================== TRANSFORMAÇÕES DE DATAFRAMES ORIGINAIS ====================
        
        # 1. df_transacoes - Rewards Transactions
        colunas_descartar = ['Wallet ID', 'Updated At']
        for col in colunas_descartar:
            if col in df_transacoes.columns:
                df_transacoes = df_transacoes.drop(columns=[col])
        
        renames_transacoes = {'Price': 'Points', 'ID': 'Transaction ID'}
        for old_name, new_name in renames_transacoes.items():
            if old_name in df_transacoes.columns:
                df_transacoes = df_transacoes.rename(columns={old_name: new_name})
        
        if 'Created At' in df_transacoes.columns:
            df_transacoes['Transaction Created At'] = pd.to_datetime(df_transacoes['Created At'], errors='coerce')
        
        # 2. df_product - Products (MANTER Type e Metadata, extrair product_points)
        colunas_descartar = ['Description', 'Cover Picture URL', 'Hash', 'Tags', 'Redeemable']
        for col in colunas_descartar:
            if col in df_product.columns:
                df_product = df_product.drop(columns=[col])
        
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
            df_product['product_points'] = df_product['Metadata'].apply(extrair_pontos_metadata)
        else:
            df_product['product_points'] = 0
        
        renames_product = {'ID': 'Product ID'}
        for old_name, new_name in renames_product.items():
            if old_name in df_product.columns:
                df_product = df_product.rename(columns={old_name: new_name})
        
        # 3. df_subscription - Subscriptions
        colunas_descartar = ['Updated At']
        for col in colunas_descartar:
            if col in df_subscription.columns:
                df_subscription = df_subscription.drop(columns=[col])
        
        renames_subscription = {'ID': 'Subscription ID'}
        for old_name, new_name in renames_subscription.items():
            if old_name in df_subscription.columns:
                df_subscription = df_subscription.rename(columns={old_name: new_name})
        
        if 'Created At' in df_subscription.columns:
            df_subscription['Subscription Created At'] = pd.to_datetime(df_subscription['Created At'], errors='coerce')
        
        # Garantir conversão da coluna Start Date para datetime
        if 'Start Date' in df_subscription.columns:
            df_subscription['Start Date'] = pd.to_datetime(df_subscription['Start Date'], errors='coerce')
        
        # 4. df_boost - Boosts
        colunas_descartar = ['Con Figs', 'Cover Picture URL', 'Description', 'Allow Points Purchase']
        for col in colunas_descartar:
            if col in df_boost.columns:
                df_boost = df_boost.drop(columns=[col])
        
        renames_boost = {'ID': 'Boost ID', 'Name': 'Name Boost'}
        for old_name, new_name in renames_boost.items():
            if old_name in df_boost.columns:
                df_boost = df_boost.rename(columns={old_name: new_name})
        
        # 5. df_partner - Partners
        colunas_descartar = ['Logo URL', 'Settings', 'Discord Guild ID', 'Description', 
                           'Discord URL', 'Insta Gram URL', 'Modalities', 'Site URL', 'Twitch URL']
        for col in colunas_descartar:
            if col in df_partner.columns:
                df_partner = df_partner.drop(columns=[col])
        
        renames_partner = {'ID': 'Partner ID', 'Name': 'Partner Name'}
        for old_name, new_name in renames_partner.items():
            if old_name in df_partner.columns:
                df_partner = df_partner.rename(columns={old_name: new_name})
        
        # 6. df_user_product - User Products
        colunas_descartar = ['Serial Number', 'Updated At', 'Opened']
        for col in colunas_descartar:
            if col in df_user_product.columns:
                df_user_product = df_user_product.drop(columns=[col])
        
        renames_user_product = {'ID': 'User Product ID', 'Created At': 'User Product Created At'}
        for old_name, new_name in renames_user_product.items():
            if old_name in df_user_product.columns:
                df_user_product = df_user_product.rename(columns={old_name: new_name})
        
        # 7. df_campaign - Campaigns
        colunas_descartar = ['Description', 'Cover Picture URL', 'Start Date', 'Finish Date', 
                           'Status', 'Highlight', 'Premium', 'Sponsored', 'Updated At']
        for col in colunas_descartar:
            if col in df_campaign.columns:
                df_campaign = df_campaign.drop(columns=[col])
        
        renames_campaign = {'ID': 'Campaign ID', 'Created At': 'Campaign Created At', 'Name': 'Campaign Name'}
        for old_name, new_name in renames_campaign.items():
            if old_name in df_campaign.columns:
                df_campaign = df_campaign.rename(columns={old_name: new_name})
        
        # 8. df_campaign_user - Campaign Users
        colunas_descartar = ['Updated At', 'Status']
        for col in colunas_descartar:
            if col in df_campaign_user.columns:
                df_campaign_user = df_campaign_user.drop(columns=[col])
        
        renames_campaign_user = {'ID': 'Campaign User ID', 'Created At': 'Campaign User Created At'}
        for old_name, new_name in renames_campaign_user.items():
            if old_name in df_campaign_user.columns:
                df_campaign_user = df_campaign_user.rename(columns={old_name: new_name})
        
        # 9. df_campaign_quest - Campaign Quests
        colunas_descartar = ['Updated At']
        for col in colunas_descartar:
            if col in df_campaign_quest.columns:
                df_campaign_quest = df_campaign_quest.drop(columns=[col])
        
        renames_campaign_quest = {'ID': 'Campaign Quest ID', 'Created At': 'Campaign Quest Created At'}
        for old_name, new_name in renames_campaign_quest.items():
            if old_name in df_campaign_quest.columns:
                df_campaign_quest = df_campaign_quest.rename(columns={old_name: new_name})
        
        # 10. df_reward - Rewards
        renames_reward = {'ID': 'Reward ID'}
        for old_name, new_name in renames_reward.items():
            if old_name in df_reward.columns:
                df_reward = df_reward.rename(columns={old_name: new_name})
        
        # 11. df_user - Users (COM FAIXA ETÁRIA)
        colunas_descartar_user = ['Email', 'Lottery Numbers', 'Pin', 'Full Name', 
                                'Banner Picture URL', 'Profile Picture URL', 'User Preferences']
        for col in colunas_descartar_user:
            if col in df_user.columns:
                df_user = df_user.drop(columns=[col])
        
        renames_user = {'Score': 'Actual Points', 'ID': 'User ID'}
        for old_name, new_name in renames_user.items():
            if old_name in df_user.columns:
                df_user = df_user.rename(columns={old_name: new_name})
        
        # Criar Faixa Etária conforme especificado
        if 'Birth Date' in df_user.columns:
            try:
                df_user['Birth Date'] = pd.to_datetime(df_user['Birth Date'], errors='coerce')
                current_year = datetime.now().year
                df_user['Age'] = current_year - df_user['Birth Date'].dt.year
                age_bins = [0, 18, 24, 34, 44, 54, 64, 100]
                age_labels = ['<18', '18-24', '25-34', '35-44', '45-54', '55-64', '65+']
                df_user['Faixa_Etaria'] = pd.cut(df_user['Age'], bins=age_bins, labels=age_labels, right=False)
            except Exception as e:
                df_user['Faixa_Etaria'] = 'Não informado'
        
        if 'Created At' in df_user.columns:
            df_user['User Created At'] = pd.to_datetime(df_user['Created At'], errors='coerce')
        
        # 12. df_user_partner_score - User Partner Scores
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

def fazer_merge_rewards(df_transacoes, df_user_product, df_product, df_partner, df_user, df_user_partner_score):
    """
    MERGE CORRIGIDO: Realiza o merge das tabelas de transações de rewards.
    OBRIGATÓRIO: Sempre usar 'User ID' como chave primária de junção.
    """
    
    # CORREÇÃO FUNDAMENTAL: Merge inicial deve ser SEMPRE por 'User ID'
    # Isso garante consistência nos dados e evita inflação por Store Product ID
    df_merged1 = pd.merge(df_transacoes, df_user_product, on='User ID', how='inner')
    
    if 'Product ID' in df_merged1.columns and 'Product ID' in df_product.columns:
        df_merged2 = pd.merge(df_merged1, df_product, on='Product ID', how='left', suffixes=('', '_prod'))
    else:
        df_merged2 = df_merged1
    
    if 'Partner ID' in df_merged2.columns and 'Partner ID' in df_partner.columns:
        df_merged3 = pd.merge(df_merged2, df_partner, on='Partner ID', how='left')
    else:
        df_merged3 = df_merged2
    
    if 'User ID' in df_merged3.columns and 'User ID' in df_user.columns:
        user_cols = ['User ID', 'Username', 'Actual Points', 'Faixa_Etaria']
        if 'Age' in df_user.columns:
            user_cols.append('Age')
        df_merged4 = pd.merge(df_merged3, df_user[user_cols], on='User ID', how='left', suffixes=('', '_user'))
    else:
        df_merged4 = df_merged3
    
    if 'User ID' in df_merged4.columns and 'Partner ID' in df_merged4.columns:
        if 'User ID' in df_user_partner_score.columns and 'Partner ID' in df_user_partner_score.columns:
            df_final_rewards = pd.merge(df_merged4, df_user_partner_score, on=['User ID', 'Partner ID'], 
                                       how='left', suffixes=('', '_partner_score'))
        else:
            df_final_rewards = df_merged4
    else:
        df_final_rewards = df_merged4

    return df_final_rewards

def fazer_merge_boosts(df_subscription, df_boost, df_partner, df_user):
    """Realiza o merge dos DataFrames para análise de boosts baseado em subscriptions."""
    
    # Começar com subscription como base (app_v6 logic)
    df_merged1 = pd.merge(df_subscription, df_boost, on='Boost ID', how='left')
    df_merged2 = pd.merge(df_merged1, df_partner, on='Partner ID', how='left', suffixes=('', '_partner'))
    
    if 'User ID' in df_merged2.columns and 'User ID' in df_user.columns:
        user_cols = ['User ID', 'Username', 'Actual Points', 'Faixa_Etaria']
        if 'Age' in df_user.columns:
            user_cols.append('Age')
        df_boost_completo = pd.merge(df_merged2, df_user[user_cols], on='User ID', how='left', suffixes=('', '_user'))
    else:
        df_boost_completo = df_merged2
    
    # Processamento de datas baseado em Subscription Created At e Start Date
    if 'Subscription Created At' in df_boost_completo.columns:
        df_boost_completo['Subscription Created At'] = pd.to_datetime(df_boost_completo['Subscription Created At'], errors='coerce')
        df_boost_completo['semana_boost'] = df_boost_completo['Subscription Created At'].dt.to_period('W')
        df_boost_completo['data_boost'] = df_boost_completo['Subscription Created At'].dt.date
        df_boost_completo['dia_semana'] = df_boost_completo['Subscription Created At'].dt.day_name()
        
        dias_pt = {
            'Monday': 'Segunda-feira', 'Tuesday': 'Terça-feira', 
            'Wednesday': 'Quarta-feira', 'Thursday': 'Quinta-feira',
            'Friday': 'Sexta-feira', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
        }
        df_boost_completo['dia_semana'] = df_boost_completo['dia_semana'].map(dias_pt)
        df_boost_completo['data_transacao'] = df_boost_completo['Subscription Created At'].dt.date
    
    return df_boost_completo

def fazer_merge_campanhas(df_campaign, df_campaign_user, df_campaign_quest, df_reward, df_user, df_user_partner_score):
    """Realiza o merge dos DataFrames para análise de campanhas."""
    
    df_base = df_campaign_user.copy()
    df_merged1 = pd.merge(df_base, df_campaign, on='Campaign ID', how='left')
    df_merged2 = pd.merge(df_merged1, df_campaign_quest, on='Campaign ID', how='left')
    df_merged3 = pd.merge(df_merged2, df_reward, on='Campaign ID', how='left')
    
    if 'User ID' in df_merged3.columns and 'User ID' in df_user.columns:
        user_cols = ['User ID', 'Username', 'Actual Points', 'Faixa_Etaria']
        if 'Age' in df_user.columns:
            user_cols.append('Age')
        df_merged4 = pd.merge(df_merged3, df_user[user_cols], on='User ID', how='left', suffixes=('', '_user'))
    else:
        df_merged4 = df_merged3
    
    if 'User ID' in df_merged4.columns and 'Partner ID' in df_merged4.columns:
        if 'User ID' in df_user_partner_score.columns and 'Partner ID' in df_user_partner_score.columns:
            df_campanhas_completo = pd.merge(df_merged4, df_user_partner_score, on=['User ID', 'Partner ID'], 
                                            how='left', suffixes=('', '_partner_score'))
        else:
            df_campanhas_completo = df_merged4
    else:
        df_campanhas_completo = df_merged4
    
    # Processamento de datas
    if 'Campaign User Created At' in df_campanhas_completo.columns:
        try:
            df_campanhas_completo['Campaign User Created At'] = pd.to_datetime(
                df_campanhas_completo['Campaign User Created At'], errors='coerce')
            df_campanhas_completo['data_participacao'] = df_campanhas_completo['Campaign User Created At'].dt.date
            df_campanhas_completo['semana_participacao'] = df_campanhas_completo['Campaign User Created At'].dt.to_period('W')
        except Exception as e:
            pass
    
    return df_campanhas_completo

# ==================== FUNÇÕES PARA DASHBOARD GERAL ====================

def calcular_usuarios_engajados(df_rewards, df_boosts, df_campanhas, parceiro_selecionado):
    """Calcula usuários engajados seguindo a lógica especificada."""
    # Filtrar dataframes pelo parceiro se necessário
    if parceiro_selecionado != "Todos os Parceiros":
        df_rewards_filt = df_rewards[df_rewards['Partner Name'] == parceiro_selecionado] if 'Partner Name' in df_rewards.columns else pd.DataFrame()
        df_boosts_filt = df_boosts[df_boosts['Partner Name'] == parceiro_selecionado] if 'Partner Name' in df_boosts.columns else pd.DataFrame()
        df_campanhas_filt = df_campanhas[df_campanhas['Partner Name'] == parceiro_selecionado] if 'Partner Name' in df_campanhas.columns else pd.DataFrame()
    else:
        df_rewards_filt = df_rewards
        df_boosts_filt = df_boosts
        df_campanhas_filt = df_campanhas
    
    # Extrair User IDs únicos de cada DataFrame filtrado
    usuarios_rewards = set(df_rewards_filt['User ID'].dropna().unique()) if 'User ID' in df_rewards_filt.columns and len(df_rewards_filt) > 0 else set()
    usuarios_boosts = set(df_boosts_filt['User ID'].dropna().unique()) if 'User ID' in df_boosts_filt.columns and len(df_boosts_filt) > 0 else set()
    usuarios_campanhas = set(df_campanhas_filt['User ID'].dropna().unique()) if 'User ID' in df_campanhas_filt.columns and len(df_campanhas_filt) > 0 else set()
    
    # União de todos os User IDs únicos
    usuarios_engajados_set = usuarios_rewards | usuarios_boosts | usuarios_campanhas
    
    return usuarios_engajados_set

def calcular_total_pontos_gerados(df_campanhas, df_rewards, df_product, parceiro_selecionado):
    """
    FUNÇÃO ATUALIZADA: Calcula o total de pontos gerados usando a lógica corrigida.
    """
    # Filtrar por parceiro se necessário
    if parceiro_selecionado != "Todos os Parceiros":
        df_campanhas_filt = df_campanhas[df_campanhas['Partner Name'] == parceiro_selecionado] if 'Partner Name' in df_campanhas.columns else df_campanhas
        df_rewards_filt = df_rewards[df_rewards['Partner Name'] == parceiro_selecionado] if 'Partner Name' in df_rewards.columns else df_rewards
    else:
        df_campanhas_filt = df_campanhas
        df_rewards_filt = df_rewards
    
    # ==================== PONTOS DE MISSÕES (PARTNER POINTS CORRIGIDO) ====================
    if len(df_campanhas_filt) > 0 and 'Partner Points' in df_campanhas_filt.columns:
        # PASSO 1: Isolar scores únicos por usuário-parceiro
        unique_scores_df = df_campanhas_filt.drop_duplicates(subset=['User ID', 'Partner Name'])
        # PASSO 2: Somar os scores únicos
        pontos_missoes = unique_scores_df['Partner Points'].sum()
    else:
        pontos_missoes = 0
    
    # ==================== PONTOS COMPRADOS (POINTS_PACKAGE) ====================
    pontos_comprados = 0
    if 'Type' in df_rewards_filt.columns and 'Points' in df_rewards_filt.columns:
        # Filtrar transações de POINTS_PACKAGE
        points_packages = df_rewards_filt[df_rewards_filt['Type'] == 'POINTS_PACKAGE']
        pontos_comprados = points_packages['Points'].sum()
    
    total_pontos = pontos_missoes + pontos_comprados
    return total_pontos, pontos_missoes, pontos_comprados

def calcular_crescimento_assinaturas(df_boosts, parceiro_selecionado):
    """Calcula métricas de crescimento de assinaturas (WoW e MoM) - LÓGICA SIMPLIFICADA."""
    if 'Subscription Created At' not in df_boosts.columns:
        return "N/A", "N/A"
    
    # Filtrar por parceiro se necessário
    df_filtrado = df_boosts.copy()
    if parceiro_selecionado != "Todos os Parceiros" and 'Partner Name' in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado['Partner Name'] == parceiro_selecionado]
    
    if len(df_filtrado) == 0:
        return "N/A", "N/A"
    
    # Remover valores nulos de data
    df_filtrado = df_filtrado.dropna(subset=['Subscription Created At'])
    
    if len(df_filtrado) == 0:
        return "N/A", "N/A"
    
    # Calcular data de referência (data mais recente nos dados)
    data_referencia = df_filtrado['Subscription Created At'].max()
    
    # Crescimento semanal (WoW) - LÓGICA SIMPLIFICADA
    try:
        # Últimos 7 dias
        inicio_periodo_atual = data_referencia - pd.Timedelta(days=6)
        assinaturas_periodo_atual = len(df_filtrado[
            (df_filtrado['Subscription Created At'] >= inicio_periodo_atual) & 
            (df_filtrado['Subscription Created At'] <= data_referencia)
        ])
        
        # 8 a 14 dias atrás
        inicio_periodo_anterior = data_referencia - pd.Timedelta(days=13)
        fim_periodo_anterior = data_referencia - pd.Timedelta(days=7)
        assinaturas_periodo_anterior = len(df_filtrado[
            (df_filtrado['Subscription Created At'] >= inicio_periodo_anterior) & 
            (df_filtrado['Subscription Created At'] <= fim_periodo_anterior)
        ])
        
        # Calcular crescimento semanal
        if assinaturas_periodo_anterior > 0:
            crescimento_semanal = ((assinaturas_periodo_atual - assinaturas_periodo_anterior) / assinaturas_periodo_anterior) * 100
            crescimento_semanal_str = f"{crescimento_semanal:+.1f}%"
        elif assinaturas_periodo_atual > 0:
            crescimento_semanal_str = "+100%"
        else:
            crescimento_semanal_str = "0%"
    except:
        crescimento_semanal_str = "0%"
    
    # Crescimento mensal (MoM) - LÓGICA SIMPLIFICADA
    try:
        # Últimos 30 dias
        inicio_mes_atual = data_referencia - pd.Timedelta(days=29)
        assinaturas_mes_atual = len(df_filtrado[
            (df_filtrado['Subscription Created At'] >= inicio_mes_atual) & 
            (df_filtrado['Subscription Created At'] <= data_referencia)
        ])
        
        # 31 a 60 dias atrás
        inicio_mes_anterior = data_referencia - pd.Timedelta(days=59)
        fim_mes_anterior = data_referencia - pd.Timedelta(days=30)
        assinaturas_mes_anterior = len(df_filtrado[
            (df_filtrado['Subscription Created At'] >= inicio_mes_anterior) & 
            (df_filtrado['Subscription Created At'] <= fim_mes_anterior)
        ])
        
        # Calcular crescimento mensal
        if assinaturas_mes_anterior > 0:
            crescimento_mensal = ((assinaturas_mes_atual - assinaturas_mes_anterior) / assinaturas_mes_anterior) * 100
            crescimento_mensal_str = f"{crescimento_mensal:+.1f}%"
        elif assinaturas_mes_atual > 0:
            crescimento_mensal_str = "+100%"
        else:
            crescimento_mensal_str = "0%"
    except:
        crescimento_mensal_str = "0%"
    
    return crescimento_semanal_str, crescimento_mensal_str

def calcular_crescimento_pontos_semanal(df_campanhas, parceiro_selecionado):
    """Calcula crescimento semanal de Partner Points."""
    if 'Partner Points' not in df_campanhas.columns or 'Campaign User Created At' not in df_campanhas.columns:
        return "N/A"
    
    # Filtrar por parceiro se necessário
    df_filtrado = df_campanhas.copy()
    if parceiro_selecionado != "Todos os Parceiros" and 'Partner Name' in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado['Partner Name'] == parceiro_selecionado]
    
    if len(df_filtrado) == 0:
        return "N/A"
    
    # Remover valores nulos
    df_filtrado = df_filtrado.dropna(subset=['Campaign User Created At', 'Partner Points'])
    
    if len(df_filtrado) == 0:
        return "N/A"
    
    try:
        data_atual = df_filtrado['Campaign User Created At'].max()
        
        # Última semana completa
        inicio_semana_atual = data_atual - pd.Timedelta(days=data_atual.weekday() + 7)
        fim_semana_atual = inicio_semana_atual + pd.Timedelta(days=6)
        
        # Semana anterior
        inicio_semana_anterior = inicio_semana_atual - pd.Timedelta(days=7)
        fim_semana_anterior = inicio_semana_atual - pd.Timedelta(days=1)
        
        pontos_semana_atual = df_filtrado[
            (df_filtrado['Campaign User Created At'] >= inicio_semana_atual) & 
            (df_filtrado['Campaign User Created At'] <= fim_semana_atual)
        ]['Partner Points'].sum()
        
        pontos_semana_anterior = df_filtrado[
            (df_filtrado['Campaign User Created At'] >= inicio_semana_anterior) & 
            (df_filtrado['Campaign User Created At'] <= fim_semana_anterior)
        ]['Partner Points'].sum()
        
        if pontos_semana_anterior > 0:
            crescimento_pontos = ((pontos_semana_atual - pontos_semana_anterior) / pontos_semana_anterior) * 100
            return f"{crescimento_pontos:+.1f}%"
        else:
            return "N/A" if pontos_semana_atual == 0 else "+100%"
    except:
        return "N/A"

def calcular_kpis_dashboard_geral(df_rewards, df_boosts, df_campanhas, df_product, parceiro_selecionado):
    """
    FUNÇÃO ATUALIZADA: Calcula KPIs dinâmicos usando a lógica corrigida de Partner Points.
    """
    
    # Calcular usuários engajados usando a nova lógica
    usuarios_engajados_set = calcular_usuarios_engajados(df_rewards, df_boosts, df_campanhas, parceiro_selecionado)
    usuarios_engajados = len(usuarios_engajados_set)
    
    # Filtrar dataframes pelo parceiro para outros KPIs
    if parceiro_selecionado != "Todos os Parceiros":
        df_rewards_filt = df_rewards[df_rewards['Partner Name'] == parceiro_selecionado] if 'Partner Name' in df_rewards.columns else df_rewards
        df_boosts_filt = df_boosts[df_boosts['Partner Name'] == parceiro_selecionado] if 'Partner Name' in df_boosts.columns else df_boosts
        df_campanhas_filt = df_campanhas[df_campanhas['Partner Name'] == parceiro_selecionado] if 'Partner Name' in df_campanhas.columns else df_campanhas
    else:
        df_rewards_filt = df_rewards
        df_boosts_filt = df_boosts
        df_campanhas_filt = df_campanhas
    
    # ==================== PARTNER POINTS CORRIGIDO ====================
    # Aplicar a mesma lógica de 2 passos para calcular Partner Points correto
    if len(df_campanhas_filt) > 0 and 'Partner Points' in df_campanhas_filt.columns:
        # PASSO 1: Isolar scores únicos por usuário-parceiro
        unique_scores_df = df_campanhas_filt.drop_duplicates(subset=['User ID', 'Partner Name'])
        # PASSO 2: Somar os scores únicos 
        partner_points = unique_scores_df['Partner Points'].sum()
    else:
        partner_points = 0
    
    # Recompensas Resgatadas
    recompensas_resgatadas = len(df_rewards_filt)
    
    # Novas Assinaturas de Boost
    novas_assinaturas = len(df_boosts_filt)
    
    # Total de Pontos Gerados (usando lógica corrigida)
    total_pontos, pontos_missoes, pontos_comprados = calcular_total_pontos_gerados(df_campanhas, df_rewards, df_product, parceiro_selecionado)
    
    return usuarios_engajados, partner_points, recompensas_resgatadas, novas_assinaturas, total_pontos

def criar_grafico_novos_usuarios_por_semana(df_boosts):
    """
    FUNÇÃO SIMPLIFICADA: Cria gráfico de usuários únicos com novas assinaturas por semana usando dados pré-filtrados.
    """
    if 'Start Date' not in df_boosts.columns or 'User ID' not in df_boosts.columns:
        return None
    
    # Forçar conversão para datetime e remover nulos
    df_filtrado = df_boosts.copy()
    df_filtrado['Start Date'] = pd.to_datetime(df_filtrado['Start Date'], errors='coerce')
    df_filtrado = df_filtrado.dropna(subset=['Start Date', 'User ID'])
    
    if len(df_filtrado) == 0:
        return None
    
    # Filtrar últimos 30 dias
    data_limite = datetime.now() - timedelta(days=30)
    df_filtrado = df_filtrado[df_filtrado['Start Date'] >= data_limite]
    
    if len(df_filtrado) == 0:
        return None
    
    # Agrupar por semana
    df_filtrado['semana'] = df_filtrado['Start Date'].dt.to_period('W')
    usuarios_por_semana = df_filtrado.groupby('semana')['User ID'].nunique().reset_index()
    usuarios_por_semana['semana_str'] = usuarios_por_semana['semana'].astype(str)
    
    if len(usuarios_por_semana) == 0:
        return None
    
    fig = px.bar(
        usuarios_por_semana,
        x='semana_str',
        y='User ID',
        title='Usuários Únicos com Novas Assinaturas por Semana W7M (Último Mês)',
        labels={'semana_str': 'Semana', 'User ID': 'Usuários Únicos'},
        color='User ID',
        color_continuous_scale='Viridis'
    )
    
    fig.update_layout(height=400, title_x=0.5, xaxis_tickangle=-45, showlegend=False)
    
    return fig

def criar_grafico_total_assinaturas_por_boost(df_boosts):
    """
    FUNÇÃO SIMPLIFICADA: Cria gráfico de barras com total de assinaturas por tipo de boost usando dados pré-filtrados.
    """
    if 'Name Boost' not in df_boosts.columns:
        return None
    
    # Remover valores nulos
    df_filtrado = df_boosts.dropna(subset=['Name Boost'])
    
    if len(df_filtrado) == 0:
        return None
    
    # Contar assinaturas por tipo de boost
    assinaturas_por_boost = df_filtrado['Name Boost'].value_counts()
    
    if len(assinaturas_por_boost) == 0:
        return None
    
    fig = px.bar(
        x=assinaturas_por_boost.index,
        y=assinaturas_por_boost.values,
        title='Total de Assinaturas por Tipo de Boost W7M',
        labels={'x': 'Tipo de Boost', 'y': 'Total de Assinaturas'},
        color=assinaturas_por_boost.values,
        color_continuous_scale='Viridis'
    )
    
    fig.update_layout(height=400, showlegend=False, title_x=0.5, xaxis_tickangle=-45)
    
    return fig

def criar_grafico_crescimento_diario_assinaturas(df_boosts):
    """
    FUNÇÃO SIMPLIFICADA: Cria gráfico de crescimento diário de assinaturas usando dados pré-filtrados.
    """
    if 'Start Date' not in df_boosts.columns or 'Name Boost' not in df_boosts.columns:
        return None
    
    # Conversão para datetime
    df_filtrado = df_boosts.copy()
    df_filtrado['Start Date'] = pd.to_datetime(df_filtrado['Start Date'], errors='coerce')
    df_filtrado.dropna(subset=['Start Date'], inplace=True)
    
    # Remover valores nulos
    df_filtrado = df_filtrado.dropna(subset=['Name Boost'])
    
    if len(df_filtrado) == 0:
        return None
    
    # Filtrar últimos 30 dias
    data_limite = datetime.now() - timedelta(days=30)
    df_filtrado = df_filtrado[df_filtrado['Start Date'] >= data_limite]
    
    if len(df_filtrado) == 0:
        return None
    
    # Agrupar por data e tipo de boost
    assinaturas_diarias = df_filtrado.groupby([df_filtrado['Start Date'].dt.date, 'Name Boost']).size().reset_index(name='count')
    assinaturas_diarias['Start Date'] = pd.to_datetime(assinaturas_diarias['Start Date'])
    
    fig = px.line(
        assinaturas_diarias,
        x='Start Date',
        y='count',
        color='Name Boost',
        title='Novas Assinaturas de Boosts W7M (Últimos 30 Dias)',
        labels={'Start Date': 'Data', 'count': 'Novas Assinaturas', 'Name Boost': 'Tipo de Boost'},
        markers=True
    )
    
    fig.update_layout(height=400, title_x=0.5)
    
    return fig

def criar_heatmap_usuario_recompensa(df_rewards):
    """
    FUNÇÃO SIMPLIFICADA: Cria heatmap entre top 10 usuários e top 10 recompensas usando dados pré-filtrados.
    """
    # Verificar se as colunas necessárias existem
    nome_col = None
    for col in ['Name', 'Product Name']:
        if col in df_rewards.columns:
            nome_col = col
            break
    
    if not nome_col or 'Username' not in df_rewards.columns:
        return None
    
    # Limpeza de dados: remover duplicatas e valores nulos
    df_filtrado = df_rewards.copy()
    if 'Transaction ID' in df_filtrado.columns:
        df_filtrado = df_filtrado.drop_duplicates(subset=['Transaction ID'])
    
    df_filtrado = df_filtrado.dropna(subset=['Username', nome_col])
    
    if len(df_filtrado) == 0:
        return None
    
    # Encontrar top 10 usuários por contagem de resgates
    top_usuarios = df_filtrado['Username'].value_counts().head(10).index.tolist()
    
    # Encontrar top 10 recompensas por contagem de resgates
    top_recompensas = df_filtrado[nome_col].value_counts().head(10).index.tolist()
    
    # Filtrar para manter apenas top 10 usuários E top 10 recompensas
    df_top = df_filtrado[
        (df_filtrado['Username'].isin(top_usuarios)) & 
        (df_filtrado[nome_col].isin(top_recompensas))
    ]
    
    if len(df_top) == 0:
        return None
    
    # Criar tabela cruzada (crosstab) entre usuários e recompensas
    crosstab = pd.crosstab(df_top['Username'], df_top[nome_col])
    
    if crosstab.empty:
        return None
    
    # Criar heatmap usando px.imshow
    fig = px.imshow(
        crosstab.values,
        x=crosstab.columns,
        y=crosstab.index,
        title='Heatmap W7M: Top 10 Usuários vs Top 10 Recompensas',
        color_continuous_scale='viridis',
        labels={'x': 'Recompensas', 'y': 'Usuários', 'color': 'Número de Resgates'},
        aspect='auto'
    )
    
    fig.update_layout(
        height=500, 
        title_x=0.5,
        xaxis_tickangle=-45,
        xaxis_title="Top 10 Recompensas",
        yaxis_title="Top 10 Usuários"
    )
    
    # Adicionar valores no heatmap para melhor legibilidade
    fig.update_traces(texttemplate="%{z}", textfont_size=10)
    
    return fig

def criar_grafico_partner_points_tempo(df_campanhas):
    """FUNÇÃO SIMPLIFICADA: Cria gráfico de linhas de Partner Points ao longo do tempo usando dados pré-filtrados."""
    if 'Partner Points' not in df_campanhas.columns or 'semana_participacao' not in df_campanhas.columns:
        return None
    
    # Agrupar por semana
    pontos_semanais = df_campanhas.groupby('semana_participacao')['Partner Points'].sum().reset_index()
    pontos_semanais['semana_str'] = pontos_semanais['semana_participacao'].astype(str)
    
    if len(pontos_semanais) == 0:
        return None
    
    fig = px.line(
        pontos_semanais,
        x='semana_str',
        y='Partner Points',
        title='Partner Points W7M Gerados ao Longo do Tempo',
        labels={'semana_str': 'Semana', 'Partner Points': 'Partner Points'},
        markers=True
    )
    
    fig.update_traces(line_color='#FF6B6B', line_width=3, marker_size=8)
    fig.update_layout(height=400, title_x=0.5, xaxis_tickangle=-45)
    
    return fig

def criar_grafico_top5_campanhas_engajamento(df_campanhas):
    """FUNÇÃO SIMPLIFICADA: Cria gráfico de barras das Top 5 campanhas por engajamento usando dados pré-filtrados."""
    if 'Campaign Name' not in df_campanhas.columns:
        return None
    
    campanhas_count = df_campanhas['Campaign Name'].value_counts().head(5)
    
    if len(campanhas_count) == 0:
        return None
    
    fig = px.bar(
        x=campanhas_count.index,
        y=campanhas_count.values,
        title='Top 5 Campanhas W7M por Engajamento',
        labels={'x': 'Campanha', 'y': 'Número de Participações'},
        color=campanhas_count.values,
        color_continuous_scale='viridis'
    )
    
    fig.update_layout(height=400, showlegend=False, title_x=0.5, xaxis_tickangle=-45)
    
    return fig

def criar_grafico_crescimento_acumulado(df_boosts):
    """FUNÇÃO SIMPLIFICADA: Cria gráfico de área do crescimento acumulado de assinaturas usando dados pré-filtrados."""
    if 'Subscription Created At' not in df_boosts.columns:
        return None
    
    # Remover valores nulos
    df_filtrado = df_boosts.dropna(subset=['Subscription Created At'])
    
    if len(df_filtrado) == 0:
        return None
    
    # Criar coluna de semana
    df_filtrado['semana'] = df_filtrado['Subscription Created At'].dt.to_period('W')
    
    # Contar assinaturas por semana
    assinaturas_semanais = df_filtrado.groupby('semana').size().reset_index(name='count')
    
    if len(assinaturas_semanais) == 0:
        return None
    
    # Calcular crescimento acumulado
    assinaturas_semanais = assinaturas_semanais.sort_values('semana')
    assinaturas_semanais['count_acumulado'] = assinaturas_semanais['count'].cumsum()
    assinaturas_semanais['semana_str'] = assinaturas_semanais['semana'].astype(str)
    
    fig = px.area(
        assinaturas_semanais,
        x='semana_str',
        y='count_acumulado',
        title='Crescimento Acumulado de Assinaturas W7M',
        labels={'semana_str': 'Semana', 'count_acumulado': 'Assinaturas Acumuladas'},
    )
    
    fig.update_layout(height=400, title_x=0.5, xaxis_tickangle=-45)
    
    return fig

# ==================== FUNÇÕES PARA ANÁLISE DE USUÁRIO ====================

def criar_grafico_sunburst_demografico(df_campanhas):
    """Cria gráfico Sunburst com Partner Name -> Faixa Etária."""
    if 'Partner Name' not in df_campanhas.columns or 'Faixa_Etaria' not in df_campanhas.columns:
        return None
    
    # Filtrar dados válidos
    df_filtered = df_campanhas.dropna(subset=['Partner Name', 'Faixa_Etaria'])
    
    if len(df_filtered) == 0:
        return None
    
    # Criar dados para sunburst
    sunburst_data = df_filtered.groupby(['Partner Name', 'Faixa_Etaria']).size().reset_index(name='count')
    
    fig = px.sunburst(
        sunburst_data,
        path=['Partner Name', 'Faixa_Etaria'],
        values='count',
        title='Distribuição Demográfica por Parceiro',
        color='count',
        color_continuous_scale='viridis'
    )
    
    fig.update_layout(height=500, title_x=0.5)
    
    return fig

def criar_grafico_top5_valor_total_usuario(df_campanhas, df_rewards, df_boosts):
    """
    LÓGICA DEFINITIVA E FINAL: Calcula "Valor Total" com 3 componentes usando processo exato especificado.
    
    PROCESSO DE CÁLCULO OBRIGATÓRIO (na ordem exata):
    1. Partner Points (Missões) - Lógica de 2 passos para evitar duplicatas
    2. Reward Points (Recompensas) - Coluna 'Points' (ex-Price) 
    3. Boost Points (Assinaturas) - Coluna 'Points'
    
    EXEMPLO: Se usuário tem Partner Points: 36.819 + Reward Points: 25.000 + Boost Points: 8.500
    = Valor Total: 70.319
    """
    # Validação rigorosa de colunas obrigatórias
    if 'Username' not in df_campanhas.columns or 'Partner Points' not in df_campanhas.columns:
        return None
    
    if 'Username' not in df_rewards.columns or 'Points' not in df_rewards.columns:
        return None
    
    if 'Username' not in df_boosts.columns or 'Points' not in df_boosts.columns:
        return None
    
    # ==================== CÁLCULO DE PARTNER POINTS (MISSÕES) - LÓGICA CORRIGIDA ====================
    # PASSO 1: Isolar o score único de cada usuário com cada parceiro
    # Filtra para ter apenas uma entrada por usuário-parceiro, preservando o score único.
    unique_scores_df = df_campanhas.drop_duplicates(subset=['User ID', 'Partner Name'])
    
    # PASSO 2: Somar os scores únicos por usuário 
    # Agrupa por Username e SOMA os Partner Points. Isso irá somar os scores de um usuário 
    # através de todos os parceiros com quem ele interagiu.
    partner_points_por_usuario = unique_scores_df.groupby('Username')['Partner Points'].sum()
    
    # ==================== CÁLCULO DE REWARD POINTS (RECOMPENSAS) - LÓGICA CORRIGIDA ====================
    # ATENÇÃO: Use a coluna 'Points' (que veio da coluna 'Price' original) do df_rewards
    # NÃO use 'product_points' - use 'Points'
    reward_points_por_usuario = df_rewards.groupby('Username')['Points'].sum()
    
    # ==================== CÁLCULO DE BOOST POINTS (ASSINATURAS) ====================
    # Some os pontos que os usuários ganham de seus boosts
    boost_points_por_usuario = df_boosts.groupby('Username')['Points'].sum()
    
    # ==================== CALCULAR VALOR TOTAL - SOMAR AS TRÊS SÉRIES ====================
    # Combinar as três séries de dados: Partner Points + Reward Points + Boost Points
    valor_total_por_usuario = (partner_points_por_usuario
                              .add(reward_points_por_usuario, fill_value=0)
                              .add(boost_points_por_usuario, fill_value=0))
    
    # ==================== IDENTIFICAR TOP 5 USUÁRIOS ====================
    # Usar o "Valor Total" para encontrar os top 5 usuários
    top_5_usuarios = valor_total_por_usuario.nlargest(5)
    top_5_usernames = top_5_usuarios.index.tolist()
    
    if len(top_5_usernames) == 0:
        return None
    
    # ==================== PREPARAR DADOS PARA GRÁFICO EMPILHADO ====================
    # Criar DataFrames separados para cada componente dos top 5 usuários
    partner_points_top5 = partner_points_por_usuario[partner_points_por_usuario.index.isin(top_5_usernames)].reset_index(name='Partner Points')
    reward_points_top5 = reward_points_por_usuario[reward_points_por_usuario.index.isin(top_5_usernames)].reset_index(name='Reward Points')
    boost_points_top5 = boost_points_por_usuario[boost_points_por_usuario.index.isin(top_5_usernames)].reset_index(name='Boost Points')
    
    # Unir todos os dados para o gráfico (3 componentes)
    df_grafico = pd.merge(partner_points_top5, reward_points_top5, on='Username', how='outer').fillna(0)
    df_grafico = pd.merge(df_grafico, boost_points_top5, on='Username', how='outer').fillna(0)
    df_grafico['Valor Total'] = df_grafico['Partner Points'] + df_grafico['Reward Points'] + df_grafico['Boost Points']
    df_grafico = df_grafico.sort_values('Valor Total', ascending=False)
    
    # Preparar dados para o gráfico de barras empilhadas
    df_melted = pd.melt(
        df_grafico, 
        id_vars=['Username', 'Valor Total'], 
        value_vars=['Partner Points', 'Reward Points', 'Boost Points'],
        var_name='Tipo de Pontos', 
        value_name='Pontos'
    )
    
    # ==================== GRÁFICO DE BARRAS EMPILHADAS DE 3 NÍVEIS ====================
    # O gráfico deve ser empilhado, mostrando as três componentes
    fig = px.bar(
        df_melted,
        x='Username',
        y='Pontos',
        color='Tipo de Pontos',
        title='Top 5 Usuários por Valor Total (Missões + Recompensas + Boosts)',
        labels={'Username': 'Usuário', 'Pontos': 'Pontos'},
        color_discrete_map={
            'Partner Points': '#1f77b4',   # Azul - Missões
            'Reward Points': '#ff7f0e',    # Laranja - Recompensas  
            'Boost Points': '#2ca02c'      # Verde - Boosts
        }
    )
    
    # Adicionar anotações com o valor total acima de cada barra
    for i, row in df_grafico.iterrows():
        fig.add_annotation(
            x=row['Username'],
            y=row['Valor Total'] + (row['Valor Total'] * 0.05),
            text=f"Total: {row['Valor Total']:,.0f}",
            showarrow=False,
            font=dict(size=10, color="black", weight="bold")
        )
    
    fig.update_layout(height=500, title_x=0.5, xaxis_tickangle=-45)
    
    return fig

def criar_tabela_top_usuario(df_user, df_campanhas):
    """Cria tabela com informações do top usuário por Partner Points."""
    if 'Username' not in df_campanhas.columns or 'Partner Points' not in df_campanhas.columns:
        return None
    
    pontos_por_usuario = df_campanhas.groupby('Username')['Partner Points'].sum()
    if len(pontos_por_usuario) == 0:
        return None
    
    top_usuario_nome = pontos_por_usuario.idxmax()
    total_partner_points = pontos_por_usuario[top_usuario_nome]
    
    usuario_data = df_user[df_user['Username'] == top_usuario_nome]
    if usuario_data.empty:
        return None
    
    usuario = usuario_data.iloc[0]
    
    info_dict = {
        'Métrica': ['Username', 'Partner Points Total'],
        'Valor': [top_usuario_nome, f"{total_partner_points:,.0f}"]
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

# ==================== FUNÇÕES PARA ANÁLISE DE REWARDS ====================

def criar_grafico_pontos_resgatados_item(df_rewards):
    """
    CORRIGIDO: Cria gráfico de total de pontos resgatados por item usando product_points.
    """
    nome_col = None
    for col in ['Name', 'Product Name']:
        if col in df_rewards.columns:
            nome_col = col
            break
    
    if not nome_col or 'product_points' not in df_rewards.columns:
        return None
    
    # Filtrar apenas registros com product_points > 0
    df_filtrado = df_rewards[df_rewards['product_points'] > 0]
    
    if len(df_filtrado) == 0:
        return None
    
    pontos_por_item = df_filtrado.groupby(nome_col)['product_points'].sum().sort_values(ascending=False).head(10)
    
    if pontos_por_item.sum() == 0:
        return None
    
    fig = px.bar(
        x=pontos_por_item.index,
        y=pontos_por_item.values,
        title='Total de Pontos Resgatados por Item',
        labels={'x': 'Item', 'y': 'Total de Pontos'},
        color=pontos_por_item.values,
        color_continuous_scale='Blues'
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
    
    unidades_por_item = df_rewards[nome_col].value_counts().head(10)
    
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

# ==================== FUNÇÕES PARA ANÁLISE DE BOOSTS ====================

def criar_grafico_novas_assinaturas_tempo(df_boosts):
    """
    CORRIGIDO: Cria gráfico de linha das novas assinaturas de boosts ao longo do tempo.
    """
    if 'semana_boost' not in df_boosts.columns:
        return None
    
    # Verificar se existe Partner Name
    if 'Partner Name' in df_boosts.columns:
        assinaturas_semanais = df_boosts.groupby(['semana_boost', 'Partner Name']).size().reset_index(name='count')
        assinaturas_semanais['semana_str'] = assinaturas_semanais['semana_boost'].astype(str)
        
        fig = px.line(
            assinaturas_semanais,
            x='semana_str',
            y='count',
            color='Partner Name',
            title='Novas Assinaturas de Boosts ao Longo do Tempo',
            labels={'semana_str': 'Semana', 'count': 'Número de Assinaturas'},
            markers=True
        )
    else:
        assinaturas_semanais = df_boosts.groupby('semana_boost').size().reset_index(name='count')
        assinaturas_semanais['semana_str'] = assinaturas_semanais['semana_boost'].astype(str)
        
        fig = px.line(
            assinaturas_semanais,
            x='semana_str',
            y='count',
            title='Novas Assinaturas de Boosts ao Longo do Tempo',
            labels={'semana_str': 'Semana', 'count': 'Número de Assinaturas'},
            markers=True
        )
    
    if len(assinaturas_semanais) == 0:
        return None
    
    fig.update_layout(height=400, title_x=0.5, xaxis_tickangle=-45)
    
    return fig

def criar_grafico_status_boosts(df_boosts):
    """
    CORRIGIDO: Cria gráfico de pizza com distribuição de status.
    Valida se a coluna Status existe.
    """
    if 'Status' not in df_boosts.columns:
        return None
    
    # Remover valores nulos
    df_filtrado = df_boosts.dropna(subset=['Status'])
    
    if len(df_filtrado) == 0:
        return None
    
    status_count = df_filtrado['Status'].value_counts()
    
    if len(status_count) == 0:
        return None
    
    fig = px.pie(
        values=status_count.values,
        names=status_count.index,
        title='Distribuição de Assinaturas por Status',
        color_discrete_sequence=px.colors.qualitative.Vivid
    )
    
    fig.update_layout(height=400, title_x=0.5)
    
    return fig

def criar_grafico_assinaturas_dia_semana(df_boosts):
    """
    CORRIGIDO: Cria gráfico de assinaturas por dia da semana.
    """
    if 'dia_semana' not in df_boosts.columns:
        return None
    
    # Remover valores nulos
    df_filtrado = df_boosts.dropna(subset=['dia_semana'])
    
    if len(df_filtrado) == 0:
        return None
    
    ordem_dias = ['Segunda-feira', 'Terça-feira', 'Quarta-feira', 'Quinta-feira', 
                  'Sexta-feira', 'Sábado', 'Domingo']
    
    assinaturas_por_dia = df_filtrado.groupby('dia_semana').size().reindex(ordem_dias, fill_value=0)
    
    fig = px.bar(
        x=assinaturas_por_dia.index,
        y=assinaturas_por_dia.values,
        title='Novas Assinaturas por Dia da Semana',
        labels={'x': 'Dia da Semana', 'y': 'Número de Assinaturas'},
        color=assinaturas_por_dia.values,
        color_continuous_scale='Plasma'
    )
    
    fig.update_layout(height=400, showlegend=False, title_x=0.5, xaxis_tickangle=-45)
    
    return fig

def criar_grafico_top5_boosts_assinaturas(df_boosts):
    """
    CORRIGIDO: Cria gráfico dos top 5 boosts por número de assinaturas.
    """
    if 'Name Boost' not in df_boosts.columns:
        return None
    
    # Remover valores nulos
    df_filtrado = df_boosts.dropna(subset=['Name Boost'])
    
    if len(df_filtrado) == 0:
        return None
    
    assinaturas_por_boost = df_filtrado['Name Boost'].value_counts().head(5)
    
    if len(assinaturas_por_boost) == 0:
        return None
    
    fig = px.bar(
        x=assinaturas_por_boost.index,
        y=assinaturas_por_boost.values,
        title='Top 5 Boosts por Número de Assinaturas',
        labels={'x': 'Boost', 'y': 'Número de Assinaturas'},
        color=assinaturas_por_boost.values,
        color_continuous_scale='Viridis'
    )
    
    fig.update_layout(height=400, showlegend=False, title_x=0.5, xaxis_tickangle=-45)
    
    return fig

# ==================== FUNÇÕES PARA ANÁLISE DE CAMPANHAS ====================

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
            color_continuous_scale='Viridis'
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
            color_continuous_scale='Sunset'
        )
        
        fig.update_layout(height=400, showlegend=False, title_x=0.5)
        fig.update_xaxes(tickmode='linear', tick0=0, dtick=2)
        
        return fig
    except Exception:
        return None

def main():
    """Função principal que executa toda a aplicação Streamlit."""
    # Configuração da página
    st.set_page_config(
        page_title="Dashboard de Análise - Parceiro W7M",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    # Título principal atualizado para W7M
    st.title("📊 Dashboard de Análise - Parceiro W7M")
    st.markdown("---")
    
    # Carregar dados
    @st.cache_data
    def load_and_merge_all_data():
        (df_transacoes, df_user_product, df_product, df_boost_trans, df_boost, df_partner,
         df_campaign, df_campaign_user, df_campaign_quest, df_reward, df_user, df_user_partner_score, df_subscription) = carregar_dados()
        
        df_rewards = fazer_merge_rewards(df_transacoes, df_user_product, df_product, df_partner, df_user, df_user_partner_score)
        df_boosts = fazer_merge_boosts(df_subscription, df_boost, df_partner, df_user)
        df_campanhas = fazer_merge_campanhas(df_campaign, df_campaign_user, df_campaign_quest, df_reward, df_user, df_user_partner_score)
        
        # Adicionar Partner Name às campanhas se possível
        if 'Partner ID' in df_campanhas.columns and 'Partner ID' in df_partner.columns:
            df_campanhas = pd.merge(df_campanhas, df_partner[['Partner ID', 'Partner Name']], 
                                   on='Partner ID', how='left', suffixes=('', '_partner'))
        
        return df_rewards, df_boosts, df_campanhas, df_partner, df_user, df_user_partner_score, df_product
    
    # Carregar dados
    with st.spinner('Carregando dados do parceiro W7M...'):
        df_rewards, df_boosts, df_campanhas, df_partner, df_user, df_user_partner_score, df_product = load_and_merge_all_data()
    
    # ==================== FILTRO FIXO PARA W7M ====================
    # Aplicar o filtro para 'W7M' logo após o carregamento dos dados.
    # Todos os cálculos e gráficos a partir daqui usarão estes DFs filtrados.

    partner_name = 'W7M'

    df_rewards_w7m = df_rewards[df_rewards['Partner Name'] == partner_name].copy()
    df_boosts_w7m = df_boosts[df_boosts['Partner Name'] == partner_name].copy()
    df_campanhas_w7m = df_campanhas[df_campanhas['Partner Name'] == partner_name].copy()

    # O df_user não precisa ser filtrado aqui, pois ele é usado para enriquecer os outros.
    # ============================================================
    
    # CRIAR ABAS COM NOVA ESTRUTURA
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
        
        # KPIs Dinâmicos usando dados filtrados
        parceiro_selecionado = 'W7M'
        usuarios_engajados, partner_points, recompensas_resgatadas, novas_assinaturas, total_pontos = \
            calcular_kpis_dashboard_geral(df_rewards_w7m, df_boosts_w7m, df_campanhas_w7m, df_product,
                                          parceiro_selecionado)
        
        # Métricas de crescimento usando dados filtrados
        crescimento_semanal, crescimento_mensal = calcular_crescimento_assinaturas(df_boosts_w7m, parceiro_selecionado)
        crescimento_pontos = calcular_crescimento_pontos_semanal(df_campanhas_w7m, parceiro_selecionado)
        
        # KPIs para W7M
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("👥 Usuários Engajados W7M", f"{usuarios_engajados:,}")
        with col2:
            st.metric("💎 Partner Points W7M", f"{partner_points:,.0f}")
        with col3:
            st.metric("🎁 Recompensas Resgatadas", f"{recompensas_resgatadas:,}")
        with col4:
            st.metric("🚀 Assinaturas de Boost", f"{novas_assinaturas:,}")
        with col5:
            st.metric("⭐ Total de Pontos Gerados", f"{total_pontos:,.0f}")
        
        # Segunda linha de métricas de crescimento
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📈 Crescimento Semanal (Assinaturas)", crescimento_semanal)
        with col2:
            st.metric("📊 Crescimento Mensal (Assinaturas)", crescimento_mensal)
        with col3:
            st.metric("💰 Crescimento Semanal (Pontos)", crescimento_pontos)
        
        st.markdown("---")
        
        # Gráficos usando dados filtrados
        fig_novos_usuarios = criar_grafico_novos_usuarios_por_semana(df_boosts_w7m)
        if fig_novos_usuarios:
            st.plotly_chart(fig_novos_usuarios, use_container_width=True)
        else:
            st.info("Dados de novos usuários não disponíveis para W7M")
        
        # Gráficos Principais
        col1, col2 = st.columns(2)
        
        with col1:
            fig_partner_points = criar_grafico_partner_points_tempo(df_campanhas_w7m)
            if fig_partner_points:
                st.plotly_chart(fig_partner_points, use_container_width=True)
            else:
                st.info("Dados de Partner Points não disponíveis")
        
        with col2:
            fig_top_campanhas = criar_grafico_top5_campanhas_engajamento(df_campanhas_w7m)
            if fig_top_campanhas:
                st.plotly_chart(fig_top_campanhas, use_container_width=True)
            else:
                st.info("Dados de campanhas não disponíveis")
        
        # Análises adicionais
        st.markdown("---")
        col1, col2 = st.columns(2)
        
        with col1:
            fig_crescimento_diario = criar_grafico_crescimento_diario_assinaturas(df_boosts_w7m)
            if fig_crescimento_diario:
                st.plotly_chart(fig_crescimento_diario, use_container_width=True)
            else:
                st.info("Dados de crescimento diário não disponíveis")
        
        with col2:
            fig_total_boosts = criar_grafico_total_assinaturas_por_boost(df_boosts_w7m)
            if fig_total_boosts:
                st.plotly_chart(fig_total_boosts, use_container_width=True)
            else:
                st.info("Dados de total por boost não disponíveis")
        
        # Gráfico de crescimento acumulado
        fig_crescimento = criar_grafico_crescimento_acumulado(df_boosts_w7m)
        if fig_crescimento:
            st.plotly_chart(fig_crescimento, use_container_width=True)
        else:
            st.info("Dados de crescimento acumulado não disponíveis")
        
        # Heatmap para W7M
        st.markdown("---")
        st.subheader("🔥 Heatmap de Engajamento W7M: Top Usuários vs Top Recompensas")
        
        fig_heatmap = criar_heatmap_usuario_recompensa(df_rewards_w7m)
        if fig_heatmap:
            st.plotly_chart(fig_heatmap, use_container_width=True)
        else:
            st.info("Dados insuficientes para gerar heatmap de usuário-recompensa")
    
    # ==================== ANÁLISE DE USUÁRIO W7M ====================
    with tab_usuario:
        st.header("👤 Análise de Usuário W7M")
        st.caption("Perfil e comportamento dos usuários do parceiro W7M")
        
        # Calcular usuários engajados usando dados W7M
        usuarios_engajados_set = calcular_usuarios_engajados(df_rewards_w7m, df_boosts_w7m, df_campanhas_w7m, parceiro_selecionado)
        
        # Métricas de usuário W7M
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_usuarios = len(usuarios_engajados_set)
            st.metric("Total de Usuários W7M", f"{total_usuarios:,}")
        
        with col2:
            if 'Partner Points' in df_campanhas_w7m.columns and len(df_campanhas_w7m) > 0:
                media_partner_points = df_campanhas_w7m['Partner Points'].mean()
                st.metric("Média Partner Points/Usuário", f"{media_partner_points:,.1f}")
            else:
                st.metric("Média Partner Points/Usuário", "0")
        
        with col3:
            if 'Actual Points' in df_user.columns and len(usuarios_engajados_set) > 0:
                usuarios_ativos = len(df_user[(df_user['User ID'].isin(usuarios_engajados_set)) & (df_user['Actual Points'] > 0)])
                st.metric("Usuários Ativos W7M", f"{usuarios_ativos:,}")
            else:
                st.metric("Usuários Ativos W7M", "0")
        
        st.markdown("---")
        
        # Visualizações Avançadas W7M
        col1, col2 = st.columns(2)
        
        with col1:
            fig_sunburst = criar_grafico_sunburst_demografico(df_campanhas_w7m)
            if fig_sunburst:
                st.plotly_chart(fig_sunburst, use_container_width=True)
            else:
                st.info("Dados demográficos não disponíveis para W7M")
        
        with col2:
            # Gráfico Top 5 Usuários W7M com 3 componentes
            fig_stacked = criar_grafico_top5_valor_total_usuario(df_campanhas_w7m, df_rewards_w7m, df_boosts_w7m)
            if fig_stacked:
                st.plotly_chart(fig_stacked, use_container_width=True)
            else:
                st.info("Dados de usuários não disponíveis para W7M")
        
        # Tabela do Top Usuário W7M
        st.markdown("---")
        st.subheader("🌟 Destaque W7M: Top Usuário por Partner Points")
        
        tabela_top = criar_tabela_top_usuario(df_user, df_campanhas_w7m)
        if tabela_top is not None:
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                st.dataframe(tabela_top, use_container_width=True, hide_index=True)
        else:
            st.info("Dados do top usuário não disponíveis para W7M")
    
    # ==================== ANÁLISE DE REWARDS W7M ====================
    with tab_rewards:
        st.header("🎁 Análise de Rewards W7M")
        st.caption("Análise detalhada de recompensas resgatadas no parceiro W7M")
        
        # KPIs W7M
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_transacoes = len(df_rewards_w7m)
            st.metric("Total de Transações W7M", f"{total_transacoes:,}")
        
        with col2:
            usuarios_unicos = df_rewards_w7m['User ID'].nunique() if 'User ID' in df_rewards_w7m.columns else 0
            st.metric("Usuários Únicos W7M", f"{usuarios_unicos:,}")
        
        with col3:
            # Usar Points ao invés de product_points para W7M
            if 'Transaction ID' in df_rewards_w7m.columns:
                df_rewards_w7m_clean = df_rewards_w7m.drop_duplicates(subset=['Transaction ID'])
            else:
                df_rewards_w7m_clean = df_rewards_w7m
                
            total_points_distribuidos = df_rewards_w7m_clean['Points'].sum() if 'Points' in df_rewards_w7m_clean.columns else 0
            st.metric("Total de Pontos W7M", f"{total_points_distribuidos:,.0f}")
        
        st.markdown("---")
        
        # Gráficos principais W7M
        col1, col2 = st.columns(2)
        
        with col1:
            fig_pontos = criar_grafico_pontos_resgatados_item(df_rewards_w7m)
            if fig_pontos:
                st.plotly_chart(fig_pontos, use_container_width=True)
            else:
                st.info("Dados de pontos não disponíveis para W7M")
        
        with col2:
            fig_unidades = criar_grafico_unidades_resgatadas_item(df_rewards_w7m)
            if fig_unidades:
                st.plotly_chart(fig_unidades, use_container_width=True)
            else:
                st.info("Dados de unidades não disponíveis para W7M")
    
    # ==================== ANÁLISE DE BOOSTS W7M ====================
    with tab_boosts:
        st.header("🚀 Análise de Boosts W7M")
        st.caption("Análise detalhada de assinaturas de boost do parceiro W7M")
        
        # KPIs W7M
        col1, col2 = st.columns(2)
        
        with col1:
            total_assinaturas = len(df_boosts_w7m)
            st.metric("📊 Total de Assinaturas W7M", f"{total_assinaturas:,}")
        
        with col2:
            usuarios_unicos = df_boosts_w7m['User ID'].nunique() if 'User ID' in df_boosts_w7m.columns else 0
            st.metric("👥 Usuários Únicos W7M", f"{usuarios_unicos:,}")
        
        st.markdown("---")
        
        # Gráfico de série temporal W7M
        fig_serie = criar_grafico_novas_assinaturas_tempo(df_boosts_w7m)
        if fig_serie:
            st.plotly_chart(fig_serie, use_container_width=True)
        else:
            st.info("Dados de série temporal não disponíveis para W7M")
        
        # Gráficos W7M
        col1, col2 = st.columns(2)
        
        with col1:
            fig_status = criar_grafico_status_boosts(df_boosts_w7m)
            if fig_status:
                st.plotly_chart(fig_status, use_container_width=True)
            else:
                st.info("Dados de status não disponíveis para W7M")
        
        with col2:
            fig_dia_semana = criar_grafico_assinaturas_dia_semana(df_boosts_w7m)
            if fig_dia_semana:
                st.plotly_chart(fig_dia_semana, use_container_width=True)
            else:
                st.info("Dados de dia da semana não disponíveis para W7M")
        
        # Gráfico dos top boosts W7M
        fig_top5 = criar_grafico_top5_boosts_assinaturas(df_boosts_w7m)
        if fig_top5:
            st.plotly_chart(fig_top5, use_container_width=True)
        else:
            st.info("Dados de top boosts não disponíveis para W7M")
    
    # ==================== ANÁLISE DE CAMPANHAS W7M ====================
    with tab_campaigns:
        st.header("🎯 Análise de Campanhas W7M")
        st.caption("Análise detalhada de engajamento em campanhas do parceiro W7M")
        
        # KPIs W7M
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_participacoes = len(df_campanhas_w7m)
            st.metric("Total de Participações W7M", f"{total_participacoes:,}")
        
        with col2:
            usuarios_unicos = df_campanhas_w7m['User ID'].nunique() if 'User ID' in df_campanhas_w7m.columns else 0
            st.metric("Usuários Únicos W7M", f"{usuarios_unicos:,}")
        
        with col3:
            total_partner_points = df_campanhas_w7m['Partner Points'].sum() if 'Partner Points' in df_campanhas_w7m.columns else 0
            st.metric("Partner Points Total W7M", f"{total_partner_points:,.0f}")
        
        st.markdown("---")
        
        # Gráfico de série temporal W7M
        if 'data_participacao' in df_campanhas_w7m.columns:
            participacoes_diarias = df_campanhas_w7m.groupby('data_participacao').size().reset_index(name='participacoes')
            participacoes_diarias = participacoes_diarias.sort_values('data_participacao')
            
            if len(participacoes_diarias) > 0:
                fig = px.line(
                    participacoes_diarias,
                    x='data_participacao',
                    y='participacoes',
                    title='Participações em Campanhas W7M ao Longo do Tempo',
                    labels={'data_participacao': 'Data', 'participacoes': 'Número de Participações'}
                )
                
                fig.update_traces(line_color='#4ECDC4', line_width=3)
                fig.update_layout(height=400, title_x=0.5)
                
                st.plotly_chart(fig, use_container_width=True)
        
        # Análises temporais de engajamento W7M
        st.markdown("---")
        st.subheader("⏰ Análises Temporais de Engajamento W7M")
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_dia_semana = criar_grafico_engajamento_dia_semana(df_campanhas_w7m)
            if fig_dia_semana:
                st.plotly_chart(fig_dia_semana, use_container_width=True)
            else:
                st.info("Dados de engajamento por dia não disponíveis para W7M")
        
        with col2:
            fig_por_hora = criar_grafico_engajamento_por_hora(df_campanhas_w7m)
            if fig_por_hora:
                st.plotly_chart(fig_por_hora, use_container_width=True)
            else:
                st.info("Dados de engajamento por hora não disponíveis para W7M")

if __name__ == "__main__":
    main()