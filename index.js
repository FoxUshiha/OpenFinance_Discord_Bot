// index.js - OpenFinance Discord Bot
// Sistema bancário completo com saldo digital, empréstimos e rendimentos

require('dotenv').config();
const { Client, GatewayIntentBits, REST, Routes, SlashCommandBuilder, 
        EmbedBuilder, ActionRowBuilder, ButtonBuilder, ButtonStyle, 
        Collection } = require('discord.js');
const sqlite3 = require('sqlite3').verbose();
const { open } = require('sqlite');
const axios = require('axios');
const crypto = require('crypto');
const fs = require('fs-extra');
const path = require('path');
const EventEmitter = require('events');

// ===================== CONFIGURAÇÃO =====================
const CONFIG = {
    // Discord
    APP_ID: process.env.APP_ID || 'ID',
    TOKEN: process.env.TOKEN || 'TOKEN',
    
    // API
    API_URL: process.env.API_URL || 'https://bank.foxsrv.net',
    GLOBAL_CARD: process.env.GLOBAL_CARD || 'CARD',
    WITHDRAW_TAX: parseFloat(process.env.WITHDRAW_TAX) || 0.001,
    
    // Performance
    API_QUEUE_INTERVAL: parseInt(process.env.API_QUEUE_INTERVAL) || 1100,
    SCORE_QUEUE_INTERVAL: parseInt(process.env.SCORE_QUEUE_INTERVAL) || 100,
    DM_QUEUE_INTERVAL: parseInt(process.env.DM_QUEUE_INTERVAL) || 2000,
    LOAN_CHECK_INTERVAL: parseInt(process.env.LOAN_CHECK_INTERVAL) || 300000,
    
    // Limites
    MAX_SCORE: 1000,
    MIN_SCORE: 0,
    SCORE_DEPOSIT_SMALL: 1,
    SCORE_DEPOSIT_MEDIUM: 5,
    SCORE_DEPOSIT_LARGE: 10,
    SCORE_WITHDRAW: -1,
    SCORE_LOAN_PAID: 10,
    SCORE_LOAN_MISSED: -10,
    SCORE_GUILD_FAIL: -10,
    SCORE_GUILD_SUCCESS: 1,
    
    // Thresholds
    MEDIUM_DEPOSIT: 0.00001000,
    LARGE_DEPOSIT: 1.0,
    
    // Decimais
    DECIMALS: 8
};

// ===================== DATABASE =====================
let db;
async function initializeDatabase() {
    db = await open({
        filename: './finance.db',
        driver: sqlite3.Database
    });

    await db.exec(`
        -- Guilds (Bancos)
        CREATE TABLE IF NOT EXISTS guilds (
            id TEXT PRIMARY KEY,
            name TEXT,
            card_id TEXT,
            log_channel TEXT,
            global_log_channel TEXT,
            approve_channel TEXT,
            tax_rate REAL DEFAULT 1.0,
            interest_rate REAL DEFAULT 0.1,
            score INTEGER DEFAULT 500,
            total_clients INTEGER DEFAULT 0,
            total_deposits TEXT DEFAULT '0',
            total_withdrawals TEXT DEFAULT '0',
            total_loans_given TEXT DEFAULT '0',
            total_interest_earned TEXT DEFAULT '0',
            total_investment_paid TEXT DEFAULT '0',
            created_at INTEGER,
            last_interest_paid INTEGER
        );

        -- Usuários
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            username TEXT,
            card_id TEXT,
            global_score INTEGER DEFAULT 500,
            total_deposits TEXT DEFAULT '0',
            total_withdrawals TEXT DEFAULT '0',
            total_loans TEXT DEFAULT '0',
            total_loans_paid TEXT DEFAULT '0',
            total_interest_paid TEXT DEFAULT '0',
            accounts_count INTEGER DEFAULT 0,
            created_at INTEGER,
            last_deposit_bonus INTEGER DEFAULT 0
        );

        -- Contas em guilds (saldo digital)
        CREATE TABLE IF NOT EXISTS accounts (
            user_id TEXT,
            guild_id TEXT,
            balance TEXT DEFAULT '0',
            joined_at INTEGER,
            last_interest_paid INTEGER,
            PRIMARY KEY (user_id, guild_id),
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (guild_id) REFERENCES guilds(id)
        );

        -- Empréstimos
        CREATE TABLE IF NOT EXISTS loans (
            id TEXT PRIMARY KEY,
            user_id TEXT,
            guild_id TEXT,
            amount TEXT,
            total_amount TEXT,
            installments INTEGER,
            paid_installments INTEGER DEFAULT 0,
            installment_value TEXT,
            interest_rate REAL,
            status TEXT DEFAULT 'pending',
            created_at INTEGER,
            approved_at INTEGER,
            approved_by TEXT,
            last_payment INTEGER,
            next_payment INTEGER,
            metadata TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (guild_id) REFERENCES guilds(id)
        );

        -- Parcelas
        CREATE TABLE IF NOT EXISTS installments (
            id TEXT PRIMARY KEY,
            loan_id TEXT,
            number INTEGER,
            amount TEXT,
            due_date INTEGER,
            paid_date INTEGER,
            status TEXT DEFAULT 'pending',
            attempts INTEGER DEFAULT 0,
            last_attempt INTEGER,
            FOREIGN KEY (loan_id) REFERENCES loans(id)
        );

        -- Staff roles
        CREATE TABLE IF NOT EXISTS staff_roles (
            guild_id TEXT,
            role_id TEXT,
            PRIMARY KEY (guild_id, role_id),
            FOREIGN KEY (guild_id) REFERENCES guilds(id)
        );

        -- Cache para limpeza (logs antigos)
        CREATE TABLE IF NOT EXISTS message_cache (
            id TEXT PRIMARY KEY,
            type TEXT,
            data TEXT,
            created_at INTEGER
        );

        -- Estatísticas
        CREATE TABLE IF NOT EXISTS stats (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at INTEGER
        );

        -- Índices
        CREATE INDEX IF NOT EXISTS idx_loans_status ON loans(status);
        CREATE INDEX IF NOT EXISTS idx_installments_due ON installments(status, due_date);
        CREATE INDEX IF NOT EXISTS idx_accounts_user ON accounts(user_id);
        CREATE INDEX IF NOT EXISTS idx_accounts_guild ON accounts(guild_id);
        CREATE INDEX IF NOT EXISTS idx_message_cache_created ON message_cache(created_at);
    `);

    const stats = ['total_guilds', 'total_users', 'total_loans', 'total_volume'];
    for (const stat of stats) {
        await db.run('INSERT OR IGNORE INTO stats (key, value, updated_at) VALUES (?, ?, ?)',
            stat, '0', Date.now());
    }
}

// ===================== UTILITIES =====================
function formatAmount(amount) {
    if (amount === null || amount === undefined) return 0;
    if (typeof amount === 'string') {
        amount = parseInt(amount) / Math.pow(10, CONFIG.DECIMALS);
    }
    return amount;
}

function storeAmount(amount) {
    return Math.floor(amount * Math.pow(10, CONFIG.DECIMALS)).toString();
}

function truncateAmount(amount) {
    return Math.floor(amount * Math.pow(10, CONFIG.DECIMALS)) / Math.pow(10, CONFIG.DECIMALS);
}

function generateId() {
    return crypto.randomUUID();
}

function formatNumber(num) {
    if (num === null || num === undefined) return '0.00000000';
    const parsedNum = typeof num === 'string' ? parseFloat(num) : num;
    if (isNaN(parsedNum)) return '0.00000000';
    
    // Se for muito grande, provavelmente é valor armazenado
    let value = parsedNum;
    if (value > 1000000) { 
        value = value / Math.pow(10, CONFIG.DECIMALS);
    }
    
    return value.toFixed(CONFIG.DECIMALS);
}

function parseLoanValue(value) {
    if (value === null || value === undefined) return 0;
    if (typeof value === 'string') {
        return parseInt(value) / Math.pow(10, CONFIG.DECIMALS);
    }
    return value;
}

// ===================== QUEUE SYSTEM =====================
class Queue extends EventEmitter {
    constructor(name, interval, processor) {
        super();
        this.name = name;
        this.interval = interval;
        this.processor = processor;
        this.queue = [];
        this.processing = false;
    }

    add(item) {
        this.queue.push(item);
        this.start();
    }

    addBulk(items) {
        this.queue.push(...items);
        this.start();
    }

    start() {
        if (!this.processing) {
            this.processing = true;
            this.process();
        }
    }

    async process() {
        while (this.queue.length > 0) {
            const startTime = Date.now();
            const item = this.queue.shift();
            
            try {
                await this.processor(item);
            } catch (error) {
                console.error(`Queue ${this.name} error:`, error);
                this.emit('error', { item, error });
            }

            const elapsed = Date.now() - startTime;
            if (elapsed < this.interval) {
                await new Promise(resolve => setTimeout(resolve, this.interval - elapsed));
            }
        }
        
        this.processing = false;
    }

    stop() {
        this.processing = false;
        this.queue = [];
    }

    size() {
        return this.queue.length;
    }

    clear() {
        this.queue = [];
    }
}

// ===================== API CLIENT =====================
class APIClient {
    constructor() {
        this.baseURL = CONFIG.API_URL;
        this.queue = new Queue('api', CONFIG.API_QUEUE_INTERVAL, this.processPayment.bind(this));
    }

    async processPayment(item) {
        const { fromCard, toCard, amount, resolve, reject, txId } = item;
        
        try {
            const response = await axios.post(`${this.baseURL}/api/card/pay`, {
                fromCard,
                toCard,
                amount: amount
            });

            if (response.data && response.data.success) {
                resolve({
                    success: true,
                    txId: response.data.txId || txId,
                    amount
                });
            } else {
                reject(new Error(response.data?.error || 'PAYMENT_FAILED'));
            }
        } catch (error) {
            console.error('API payment error:', error.response?.data || error.message);
            reject(error);
        }
    }

    async pay(fromCard, toCard, amount) {
        if (fromCard === toCard) {
            return Promise.reject(new Error('SAME_CARD'));
        }

        return new Promise((resolve, reject) => {
            this.queue.add({
                fromCard,
                toCard,
                amount: truncateAmount(amount),
                resolve,
                reject,
                txId: generateId()
            });
        });
    }

    async checkBalance(cardCode) {
        try {
            const response = await axios.post(`${this.baseURL}/api/card/info`, {
                cardCode
            });
            
            if (response.data && response.data.success) {
                response.data.coins = parseFloat(response.data.coins) / Math.pow(10, CONFIG.DECIMALS);
            }
            return response.data;
        } catch (error) {
            console.error('Balance check error:', error.response?.data || error.message);
            return { success: false, error: 'API_ERROR' };
        }
    }
}

const api = new APIClient();

// ===================== SCORE MANAGER =====================
class ScoreManager extends EventEmitter {
    constructor() {
        super();
        this.queue = new Queue('score', CONFIG.SCORE_QUEUE_INTERVAL, this.processScore.bind(this));
    }

    async processScore(item) {
        const { userId, change, reason, guildId, logChannel } = item;
        
        const user = await db.get('SELECT global_score FROM users WHERE id = ?', userId);
        if (!user) return;

        let newScore = user.global_score + change;
        newScore = Math.max(CONFIG.MIN_SCORE, Math.min(CONFIG.MAX_SCORE, newScore));

        await db.run('UPDATE users SET global_score = ? WHERE id = ?', newScore, userId);

        this.emit('scoreChanged', { userId, oldScore: user.global_score, newScore, change, reason });
    }

    async addChange(userId, change, reason, guildId = null, logChannel = null) {
        this.queue.add({ userId, change, reason, guildId, logChannel });
    }
}

const scoreManager = new ScoreManager();

// ===================== DM MANAGER =====================
class DMManager {
    constructor(client) {
        this.client = client;
        this.queue = new Queue('dm', CONFIG.DM_QUEUE_INTERVAL, this.processDM.bind(this));
    }

    async processDM(item) {
        const { userId, embed, content } = item;
        
        try {
            const user = await this.client.users.fetch(userId);
            if (user) {
                await user.send({ embeds: embed ? [embed] : [], content });
            }
        } catch (error) {
            console.error(`Failed to send DM to ${userId}:`, error);
        }
    }

    async send(userId, embed = null, content = null) {
        this.queue.add({ userId, embed, content });
    }
}

let dmManager;

// ===================== LOAN MANAGER =====================
class LoanManager {
    constructor(client) {
        this.client = client;
        this.checking = false;
        this.api = api;
        this.dmManager = dmManager;
        this.scoreManager = scoreManager;
    }

    async processDailyCycle() {
        if (this.checking) return;
        this.checking = true;

        try {
            console.log('🔄 Processando ciclo diário...');
            const now = Date.now();
            
            // 1. Processar parcelas vencidas (cobrar do saldo digital)
            await this.processDueInstallments(now);
            
            // 2. Processar rendimentos para todos os usuários
            await this.payInterests();
            
            // 3. Verificar empréstimos concluídos
            await this.checkCompletedLoans();

            console.log('✅ Ciclo diário concluído!');
        } catch (error) {
            console.error('Erro no ciclo diário:', error);
        } finally {
            this.checking = false;
        }
    }

    async processDueInstallments(now) {
        const installments = await db.all(`
            SELECT i.*, l.user_id, l.guild_id, l.installment_value, l.amount, l.interest_rate,
                   u.username, u.global_score,
                   g.log_channel, g.global_log_channel, g.name as guild_name,
                   g.card_id as guild_card_id
            FROM installments i
            JOIN loans l ON i.loan_id = l.id
            JOIN users u ON l.user_id = u.id
            JOIN guilds g ON l.guild_id = g.id
            WHERE i.status = 'pending' AND i.due_date <= ?
        `, now);

        for (const inst of installments) {
            await this.processInstallment(inst);
        }
    }

    async processInstallment(inst) {
        try {
            const amount = parseFloat(formatAmount(inst.installment_value));
            
            // Verificar saldo na conta digital do usuário
            const account = await db.get(`
                SELECT * FROM accounts WHERE user_id = ? AND guild_id = ?
            `, inst.user_id, inst.guild_id);

            if (!account || parseFloat(formatAmount(account.balance)) < amount) {
                await this.handleFailedPayment(inst, 'INSUFFICIENT_FUNDS');
                return;
            }

            // Deduzir do saldo digital do usuário
            const currentBalance = parseFloat(formatAmount(account.balance));
            const newBalance = currentBalance - amount;
            
            await db.run(`
                UPDATE accounts SET balance = ? WHERE user_id = ? AND guild_id = ?
            `, storeAmount(newBalance), inst.user_id, inst.guild_id);

            // Adicionar ao saldo do banco (guild)
            const guildAccount = await db.get(`
                SELECT * FROM accounts WHERE user_id = ? AND guild_id = ?
            `, inst.guild_id, inst.guild_id);

            if (guildAccount) {
                const guildBalance = parseFloat(formatAmount(guildAccount.balance));
                await db.run(`
                    UPDATE accounts SET balance = ? WHERE user_id = ? AND guild_id = ?
                `, storeAmount(guildBalance + amount), inst.guild_id, inst.guild_id);
            }

            // Marcar parcela como paga
            await db.run(`
                UPDATE installments 
                SET status = 'paid', paid_date = ? 
                WHERE id = ?
            `, Date.now(), inst.id);

            // Atualizar contador do empréstimo
            await db.run(`
                UPDATE loans 
                SET paid_installments = paid_installments + 1,
                    last_payment = ?,
                    next_payment = ?
                WHERE id = ?
            `, Date.now(), Date.now() + 86400000, inst.loan_id);

            // Adicionar score
            await this.scoreManager.addChange(inst.user_id, CONFIG.SCORE_LOAN_PAID, 
                `Parcela ${inst.number} paga em ${inst.guild_name}`);

            await this.logPayment(inst, true);

        } catch (error) {
            console.error('Erro ao processar parcela:', error);
            await this.handleFailedPayment(inst, 'PROCESSING_ERROR');
        }
    }

    async handleFailedPayment(inst, reason) {
        const attempts = inst.attempts + 1;
        const now = Date.now();

        await db.run(`
            UPDATE installments 
            SET attempts = ?, last_attempt = ?
            WHERE id = ?
        `, attempts, now, inst.id);

        await this.scoreManager.addChange(inst.user_id, CONFIG.SCORE_LOAN_MISSED, 
            `Parcela ${inst.number} não paga em ${inst.guild_name}`);

        const embed = new EmbedBuilder()
            .setColor(0xFF0000)
            .setTitle('⚠️ Pagamento não realizado')
            .setDescription(`Sua parcela do empréstimo em **${inst.guild_name}** não foi paga.`)
            .addFields(
                { name: 'Valor', value: `${formatNumber(parseLoanValue(inst.amount))} coin`, inline: true },
                { name: 'Parcela', value: `${inst.number}`, inline: true },
                { name: 'Motivo', value: this.getReasonText(reason), inline: true }
            )
            .setTimestamp();

        await this.dmManager.send(inst.user_id, embed);

        if (attempts >= 3) {
            // Empréstimo entra em default
            await db.run(`
                UPDATE loans SET status = 'defaulted' WHERE id = ?
            `, inst.loan_id);
        }

        await this.logPayment(inst, false, reason);
    }

    async payInterests() {
        const now = Date.now();
        const oneDayAgo = now - 86400000;

        // Buscar guilds que precisam pagar rendimentos
        const guilds = await db.all(`
            SELECT g.*, 
                   (SELECT COUNT(*) FROM accounts WHERE guild_id = g.id AND CAST(balance AS INTEGER) > 0) as active_accounts
            FROM guilds g
            WHERE g.last_interest_paid IS NULL OR g.last_interest_paid <= ?
        `, oneDayAgo);

        for (const guild of guilds) {
            if (guild.active_accounts === 0) {
                await db.run(`
                    UPDATE guilds SET last_interest_paid = ? WHERE id = ?
                `, now, guild.id);
                continue;
            }

            // Buscar todas as contas com saldo > 0 (excluindo a conta do banco)
            const accounts = await db.all(`
                SELECT a.*, u.username, u.global_score
                FROM accounts a
                JOIN users u ON a.user_id = u.id
                WHERE a.guild_id = ? AND CAST(a.balance AS INTEGER) > 0 AND a.user_id != ?
            `, guild.id, guild.id);

            let totalInterest = 0;

            for (const account of accounts) {
                const balance = parseFloat(formatAmount(account.balance));
                if (balance <= 0) continue;

                // Calcular rendimento (interest_rate é percentual)
                const interest = balance * (guild.interest_rate / 100);
                const newBalance = balance + interest;
                
                // Deduzir do saldo do banco
                const guildAccount = await db.get(`
                    SELECT * FROM accounts WHERE user_id = ? AND guild_id = ?
                `, guild.id, guild.id);

                if (guildAccount) {
                    const guildBalance = parseFloat(formatAmount(guildAccount.balance));
                    if (guildBalance < interest) {
                        console.log(`Banco ${guild.name} não tem saldo suficiente para pagar rendimentos`);
                        continue;
                    }
                    
                    await db.run(`
                        UPDATE accounts SET balance = ? WHERE user_id = ? AND guild_id = ?
                    `, storeAmount(guildBalance - interest), guild.id, guild.id);
                }
                
                // Adicionar rendimento à conta do usuário
                await db.run(`
                    UPDATE accounts 
                    SET balance = ?, last_interest_paid = ?
                    WHERE user_id = ? AND guild_id = ?
                `, storeAmount(newBalance), now, account.user_id, guild.id);

                totalInterest += interest;

                // Se o rendimento for significativo, dar score
                if (interest >= 0.00001000) {
                    await this.scoreManager.addChange(account.user_id, 1, 
                        `Rendimento de ${formatNumber(interest)} coin em ${guild.name}`);
                }
            }

            // Atualizar estatísticas da guild
            if (totalInterest > 0) {
                const currentTotal = parseFloat(formatAmount(guild.total_investment_paid));
                const newTotal = currentTotal + totalInterest;
                
                await db.run(`
                    UPDATE guilds 
                    SET total_investment_paid = ?,
                        last_interest_paid = ?
                    WHERE id = ?
                `, storeAmount(newTotal), now, guild.id);

                await this.logInterestPayment(guild, totalInterest);
            } else {
                await db.run(`
                    UPDATE guilds SET last_interest_paid = ? WHERE id = ?
                `, now, guild.id);
            }
        }
    }

    async checkCompletedLoans() {
        const completedLoans = await db.all(`
            SELECT * FROM loans 
            WHERE status = 'active' AND paid_installments >= installments
        `);

        for (const loan of completedLoans) {
            await this.completeLoan(loan.id);
        }
    }

    async completeLoan(loanId) {
        const loan = await db.get('SELECT * FROM loans WHERE id = ?', loanId);
        if (!loan) return;

        await db.run('UPDATE loans SET status = ? WHERE id = ?', 'completed', loanId);

        const user = await db.get('SELECT * FROM users WHERE id = ?', loan.user_id);
        if (user) {
            const totalLoansPaid = parseFloat(formatAmount(user.total_loans_paid || 0));
            const totalInterestPaid = parseFloat(formatAmount(user.total_interest_paid || 0));
            const loanAmount = parseFloat(formatAmount(loan.amount));
            const interestPaid = parseFloat(formatAmount(loan.total_amount)) - loanAmount;

            await db.run(`
                UPDATE users 
                SET total_loans_paid = ?,
                    total_interest_paid = ?
                WHERE id = ?
            `, storeAmount(totalLoansPaid + loanAmount), 
               storeAmount(totalInterestPaid + interestPaid), 
               loan.user_id);
        }

        const guild = await db.get('SELECT * FROM guilds WHERE id = ?', loan.guild_id);
        if (guild) {
            const embed = new EmbedBuilder()
                .setColor(0x00FF00)
                .setTitle('✅ Empréstimo concluído')
                .setDescription(`Empréstimo de ${user?.username || loan.user_id} foi totalmente pago!`)
                .addFields(
                    { name: 'Valor total pago', value: `${formatNumber(parseLoanValue(loan.total_amount))} coin`, inline: true },
                    { name: 'Parcelas', value: `${loan.installments}`, inline: true }
                )
                .setTimestamp();

            if (guild.log_channel) {
                try {
                    const channel = await this.client.channels.fetch(guild.log_channel);
                    if (channel) await channel.send({ embeds: [embed] });
                } catch (error) {}
            }
        }
    }

    async logPayment(inst, success, reason = null) {
        const embed = new EmbedBuilder()
            .setColor(success ? 0x00FF00 : 0xFF0000)
            .setTitle(success ? '✅ Pagamento realizado' : '❌ Falha no pagamento')
            .setDescription(`Usuário: ${inst.username}\nBanco: ${inst.guild_name}`)
            .addFields(
                { name: 'Valor', value: `${formatNumber(parseLoanValue(inst.amount))} coin`, inline: true },
                { name: 'Parcela', value: `${inst.number}`, inline: true }
            )
            .setTimestamp();

        if (!success && reason) {
            embed.addFields({ name: 'Motivo', value: this.getReasonText(reason), inline: true });
        }

        if (inst.log_channel) {
            try {
                const channel = await this.client.channels.fetch(inst.log_channel);
                if (channel) await channel.send({ embeds: [embed] });
            } catch (error) {}
        }

        if (inst.global_log_channel) {
            try {
                const channel = await this.client.channels.fetch(inst.global_log_channel);
                if (channel) await channel.send({ embeds: [embed] });
            } catch (error) {}
        }
    }

    async logInterestPayment(guild, totalInterest) {
        const embed = new EmbedBuilder()
            .setColor(0x00FF00)
            .setTitle('💰 Rendimentos pagos')
            .setDescription(`Banco: ${guild.name}`)
            .addFields(
                { name: 'Total pago', value: `${formatNumber(totalInterest)} coin`, inline: true },
                { name: 'Data', value: new Date().toLocaleString(), inline: true }
            )
            .setTimestamp();

        if (guild.log_channel) {
            try {
                const channel = await this.client.channels.fetch(guild.log_channel);
                if (channel) await channel.send({ embeds: [embed] });
            } catch (error) {}
        }
    }

    getReasonText(reason) {
        const reasons = {
            'NO_CARD': 'Usuário não possui cartão cadastrado',
            'INSUFFICIENT_FUNDS': 'Saldo insuficiente na conta digital',
            'PAYMENT_ERROR': 'Erro no processamento do pagamento',
            'SAME_CARD_ERROR': 'Erro: cartões de origem e destino são iguais',
            'PROCESSING_ERROR': 'Erro interno ao processar'
        };
        return reasons[reason] || reason;
    }

    startPeriodicCheck() {
        setInterval(() => this.processDailyCycle(), CONFIG.LOAN_CHECK_INTERVAL);
    }
}

// ===================== DISCORD CLIENT =====================
class FinanceBot {
    constructor() {
        this.client = new Client({
            intents: [GatewayIntentBits.Guilds]
        });
        
        this.commands = new Collection();
        this.loanManager = null;
        this.dmManager = null;
        this.scoreManager = scoreManager;
        this.api = api;
        
        this.setupCommands();
        this.setupEvents();
    }

    setupCommands() {
        this.commands.set('guild-card', new SlashCommandBuilder()
            .setName('guild-card')
            .setDescription('Configurar card da guild')
            .addStringOption(opt => opt.setName('card_id').setDescription('ID do card').setRequired(true))
        );

        this.commands.set('guild-log', new SlashCommandBuilder()
            .setName('guild-log')
            .setDescription('Configurar canal de log')
            .addChannelOption(opt => opt.setName('channel').setDescription('Canal de log').setRequired(true))
        );

        this.commands.set('guild-global-log', new SlashCommandBuilder()
            .setName('guild-global-log')
            .setDescription('Configurar canal de log global')
            .addChannelOption(opt => opt.setName('channel').setDescription('Canal de log global').setRequired(true))
        );

        this.commands.set('guild-tax', new SlashCommandBuilder()
            .setName('guild-tax')
            .setDescription('Configurar taxa de juros')
            .addNumberOption(opt => opt.setName('percent').setDescription('Percentual de juros').setRequired(true).setMinValue(0).setMaxValue(100))
        );

        this.commands.set('guild-interest', new SlashCommandBuilder()
            .setName('guild-interest')
            .setDescription('Configurar rendimento')
            .addNumberOption(opt => opt.setName('percent').setDescription('Percentual de rendimento').setRequired(true).setMinValue(0).setMaxValue(100))
        );

        this.commands.set('guild-aprove-channel', new SlashCommandBuilder()
            .setName('guild-aprove-channel')
            .setDescription('Configurar canal de aprovação')
            .addChannelOption(opt => opt.setName('channel').setDescription('Canal de aprovação').setRequired(true))
        );

        this.commands.set('guild-staff', new SlashCommandBuilder()
            .setName('guild-staff')
            .setDescription('Gerenciar cargos de staff')
            .addSubcommand(sub => sub.setName('add').setDescription('Adicionar cargo').addRoleOption(opt => opt.setName('role').setDescription('Cargo').setRequired(true)))
            .addSubcommand(sub => sub.setName('remove').setDescription('Remover cargo').addRoleOption(opt => opt.setName('role').setDescription('Cargo').setRequired(true)))
        );

        this.commands.set('guild-next', new SlashCommandBuilder()
            .setName('guild-next')
            .setDescription('Avançar para próximo ciclo diário (testes)')
        );

        this.commands.set('guild-name', new SlashCommandBuilder()
            .setName('guild-name')
            .setDescription('Definir nome do banco')
            .addStringOption(opt => opt.setName('name').setDescription('Nome do banco').setRequired(true))
        );

        this.commands.set('guild-deposit', new SlashCommandBuilder()
            .setName('guild-deposit')
            .setDescription('Depositar saldo digital na própria guild (ADM)')
            .addNumberOption(opt => opt.setName('amount').setDescription('Valor').setRequired(true).setMinValue(0.00000001))
        );

        this.commands.set('guild-withdraw', new SlashCommandBuilder()
            .setName('guild-withdraw')
            .setDescription('Sacar saldo digital da própria guild (ADM)')
            .addNumberOption(opt => opt.setName('amount').setDescription('Valor').setRequired(true).setMinValue(0.00000001))
        );

        this.commands.set('score', new SlashCommandBuilder()
            .setName('score')
            .setDescription('Ver score de um usuário')
            .addStringOption(opt => opt.setName('user_id').setDescription('ID do usuário').setRequired(true))
        );

        this.commands.set('finance-list', new SlashCommandBuilder()
            .setName('finance-list')
            .setDescription('Listar todos os bancos')
        );

        this.commands.set('finance-view', new SlashCommandBuilder()
            .setName('finance-view')
            .setDescription('Ver detalhes de um banco')
            .addStringOption(opt => opt.setName('banco').setDescription('ID ou nome do banco').setRequired(true))
        );

        this.commands.set('finance-deposit', new SlashCommandBuilder()
            .setName('finance-deposit')
            .setDescription('Depositar dinheiro real no banco (usa card)')
            .addStringOption(opt => opt.setName('banco').setDescription('ID do banco').setRequired(true))
            .addNumberOption(opt => opt.setName('valor').setDescription('Valor a depositar').setRequired(true).setMinValue(0.00000001))
        );

        this.commands.set('finance-withdraw', new SlashCommandBuilder()
            .setName('finance-withdraw')
            .setDescription('Sacar dinheiro real do banco (usa card)')
            .addStringOption(opt => opt.setName('banco').setDescription('ID do banco').setRequired(true))
            .addNumberOption(opt => opt.setName('valor').setDescription('Valor a sacar').setRequired(true).setMinValue(0.00000001))
        );

        this.commands.set('finance-balance', new SlashCommandBuilder()
            .setName('finance-balance')
            .setDescription('Ver saldo digital em um banco')
            .addStringOption(opt => opt.setName('banco').setDescription('ID do banco').setRequired(true))
        );

        this.commands.set('finance-loan', new SlashCommandBuilder()
            .setName('finance-loan')
            .setDescription('Gerenciar empréstimos')
            .addSubcommand(sub => sub.setName('take').setDescription('Solicitar empréstimo')
                .addStringOption(opt => opt.setName('banco').setDescription('ID do banco').setRequired(true))
                .addNumberOption(opt => opt.setName('valor').setDescription('Valor').setRequired(true).setMinValue(0.00000001))
                .addIntegerOption(opt => opt.setName('parcelas').setDescription('Número de parcelas').setRequired(true).setMinValue(1).setMaxValue(360)))
            .addSubcommand(sub => sub.setName('pay').setDescription('Pagar empréstimo')
                .addStringOption(opt => opt.setName('banco').setDescription('ID do banco').setRequired(true))
                .addStringOption(opt => opt.setName('valor').setDescription('"max" ou número de parcelas (ex: 5x)').setRequired(true)))
        );

        this.commands.set('finance-score', new SlashCommandBuilder()
            .setName('finance-score')
            .setDescription('Ver seu score e dados financeiros')
        );

        this.commands.set('finance-card', new SlashCommandBuilder()
            .setName('finance-card')
            .setDescription('Configurar seu card')
            .addStringOption(opt => opt.setName('card_id').setDescription('ID do seu card').setRequired(true))
        );
    }

    setupEvents() {
        this.client.once('ready', async () => {
            console.log(`✅ Logado como ${this.client.user.tag}`);
            
            this.dmManager = new DMManager(this.client);
            dmManager = this.dmManager;
            this.loanManager = new LoanManager(this.client);
            
            await this.registerCommands();
            
            // Criar contas para as guilds (banco)
            const guilds = this.client.guilds.cache;
            for (const [guildId, guild] of guilds) {
                await this.ensureGuild(guildId, guild.name);
                await this.ensureAccount(guildId, guildId); // Conta da própria guild
            }
            
            this.loanManager.startPeriodicCheck();
            
            console.log('🚀 Bot inicializado com sucesso!');
        });

        this.client.on('interactionCreate', async interaction => {
            if (interaction.isCommand()) {
                await this.handleCommand(interaction);
            } else if (interaction.isButton()) {
                await this.handleButton(interaction);
            }
        });
    }

    async registerCommands() {
        try {
            const rest = new REST({ version: '10' }).setToken(CONFIG.TOKEN);
            const commands = this.commands.map(cmd => cmd.toJSON());
            
            await rest.put(
                Routes.applicationCommands(CONFIG.APP_ID),
                { body: commands }
            );
            
            console.log(`📝 ${commands.length} comandos registrados globalmente`);
        } catch (error) {
            console.error('Erro ao registrar comandos:', error);
        }
    }

    async handleCommand(interaction) {
        const command = interaction.commandName;
        
        try {
            switch (command) {
                case 'guild-card': await this.handleGuildCard(interaction); break;
                case 'guild-log': await this.handleGuildLog(interaction); break;
                case 'guild-global-log': await this.handleGuildGlobalLog(interaction); break;
                case 'guild-tax': await this.handleGuildTax(interaction); break;
                case 'guild-interest': await this.handleGuildInterest(interaction); break;
                case 'guild-aprove-channel': await this.handleGuildApproveChannel(interaction); break;
                case 'guild-staff': await this.handleGuildStaff(interaction); break;
                case 'guild-next': await this.handleGuildNext(interaction); break;
                case 'guild-name': await this.handleGuildName(interaction); break;
                case 'guild-deposit': await this.handleGuildDeposit(interaction); break;
                case 'guild-withdraw': await this.handleGuildWithdraw(interaction); break;
                case 'score': await this.handleScore(interaction); break;
                case 'finance-list': await this.handleFinanceList(interaction); break;
                case 'finance-view': await this.handleFinanceView(interaction); break;
                case 'finance-deposit': await this.handleFinanceDeposit(interaction); break;
                case 'finance-withdraw': await this.handleFinanceWithdraw(interaction); break;
                case 'finance-balance': await this.handleFinanceBalance(interaction); break;
                case 'finance-loan': await this.handleFinanceLoan(interaction); break;
                case 'finance-score': await this.handleFinanceScore(interaction); break;
                case 'finance-card': await this.handleFinanceCard(interaction); break;
                default: {
                    await interaction.reply({ content: 'Comando não reconhecido.', flags: 64 });
                }
            }
        } catch (error) {
            console.error(`Erro no comando ${command}:`, error);
            
            const errorEmbed = new EmbedBuilder()
                .setColor(0xFF0000)
                .setTitle('❌ Erro')
                .setDescription('Ocorreu um erro ao processar o comando.')
                .setTimestamp();
            
            try {
                if (!interaction.replied && !interaction.deferred) {
                    await interaction.reply({ embeds: [errorEmbed], flags: 64 });
                } else {
                    await interaction.followUp({ embeds: [errorEmbed], flags: 64 });
                }
            } catch (replyError) {
                console.error('Erro ao enviar resposta de erro:', replyError);
            }
        }
    }

    async handleButton(interaction) {
        const [action, loanId] = interaction.customId.split(':');
        
        if (action === 'approve_loan') {
            await this.handleLoanApproval(interaction, loanId);
        } else if (action === 'reject_loan') {
            await this.handleLoanRejection(interaction, loanId);
        }
    }

    async ensureUser(userId, username) {
        let user = await db.get('SELECT * FROM users WHERE id = ?', userId);
        
        if (!user) {
            await db.run(`
                INSERT INTO users (id, username, global_score, created_at)
                VALUES (?, ?, ?, ?)
            `, userId, username, 500, Date.now());
            
            user = await db.get('SELECT * FROM users WHERE id = ?', userId);
        }
        
        return user;
    }

    async ensureGuild(guildId, guildName) {
        let guild = await db.get('SELECT * FROM guilds WHERE id = ?', guildId);
        
        if (!guild) {
            await db.run(`
                INSERT INTO guilds (id, name, score, created_at, tax_rate, interest_rate)
                VALUES (?, ?, ?, ?, ?, ?)
            `, guildId, guildName, 500, Date.now(), 1.0, 0.1);
            
            guild = await db.get('SELECT * FROM guilds WHERE id = ?', guildId);
        }
        
        return guild;
    }

    async ensureAccount(userId, guildId) {
        let account = await db.get(`
            SELECT * FROM accounts WHERE user_id = ? AND guild_id = ?
        `, userId, guildId);
        
        if (!account) {
            await db.run(`
                INSERT INTO accounts (user_id, guild_id, balance, joined_at)
                VALUES (?, ?, ?, ?)
            `, userId, guildId, storeAmount(0), Date.now());
            
            // Se não for a conta da própria guild, incrementar contador
            if (userId !== guildId) {
                await db.run(`
                    UPDATE guilds SET total_clients = total_clients + 1 WHERE id = ?
                `, guildId);
                
                await db.run(`
                    UPDATE users SET accounts_count = accounts_count + 1 WHERE id = ?
                `, userId);
            }
            
            account = await db.get(`
                SELECT * FROM accounts WHERE user_id = ? AND guild_id = ?
            `, userId, guildId);
        }
        
        return account;
    }

    async isStaff(guildId, member) {
        if (member.permissions.has('Administrator')) return true;
        
        const staffRoles = await db.all(`
            SELECT role_id FROM staff_roles WHERE guild_id = ?
        `, guildId);
        
        const staffRoleIds = staffRoles.map(r => r.role_id);
        
        return member.roles.cache.some(role => staffRoleIds.includes(role.id));
    }

    async handleGuildCard(interaction) {
        if (!interaction.member.permissions.has('Administrator')) {
            return await interaction.reply({ content: 'Apenas administradores podem usar este comando.', flags: 64 });
        }

        const cardId = interaction.options.getString('card_id');
        await this.ensureGuild(interaction.guildId, interaction.guild.name);
        await db.run('UPDATE guilds SET card_id = ? WHERE id = ?', cardId, interaction.guildId);

        const embed = new EmbedBuilder()
            .setColor(0x00FF00)
            .setTitle('✅ Card configurado')
            .setDescription(`Card da guild atualizado com sucesso!`)
            .addFields({ name: 'Card ID', value: "" })
            .setTimestamp();

        await interaction.reply({ embeds: [embed] });
    }

    async handleGuildLog(interaction) {
        if (!interaction.member.permissions.has('Administrator')) {
            return await interaction.reply({ content: 'Apenas administradores podem usar este comando.', flags: 64 });
        }

        const channel = interaction.options.getChannel('channel');
        await this.ensureGuild(interaction.guildId, interaction.guild.name);
        await db.run('UPDATE guilds SET log_channel = ? WHERE id = ?', channel.id, interaction.guildId);

        const embed = new EmbedBuilder()
            .setColor(0x00FF00)
            .setTitle('✅ Canal de log configurado')
            .setDescription(`Logs serão enviados em ${channel}`)
            .setTimestamp();

        await interaction.reply({ embeds: [embed] });
    }

    async handleGuildGlobalLog(interaction) {
        if (!interaction.member.permissions.has('Administrator')) {
            return await interaction.reply({ content: 'Apenas administradores podem usar este comando.', flags: 64 });
        }

        const channel = interaction.options.getChannel('channel');
        await this.ensureGuild(interaction.guildId, interaction.guild.name);
        await db.run('UPDATE guilds SET global_log_channel = ? WHERE id = ?', channel.id, interaction.guildId);

        const embed = new EmbedBuilder()
            .setColor(0x00FF00)
            .setTitle('✅ Canal de log global configurado')
            .setDescription(`Logs globais serão enviados em ${channel}`)
            .setTimestamp();

        await interaction.reply({ embeds: [embed] });
    }

    async handleGuildTax(interaction) {
        if (!interaction.member.permissions.has('Administrator')) {
            return await interaction.reply({ content: 'Apenas administradores podem usar este comando.', flags: 64 });
        }

        const percent = interaction.options.getNumber('percent');
        await this.ensureGuild(interaction.guildId, interaction.guild.name);
        await db.run('UPDATE guilds SET tax_rate = ? WHERE id = ?', percent, interaction.guildId);

        const embed = new EmbedBuilder()
            .setColor(0x00FF00)
            .setTitle('✅ Taxa de juros configurada')
            .setDescription(`Juros para empréstimos: **${percent}%**`)
            .setTimestamp();

        await interaction.reply({ embeds: [embed] });
    }

    async handleGuildInterest(interaction) {
        if (!interaction.member.permissions.has('Administrator')) {
            return await interaction.reply({ content: 'Apenas administradores podem usar este comando.', flags: 64 });
        }

        const percent = interaction.options.getNumber('percent');
        await this.ensureGuild(interaction.guildId, interaction.guild.name);
        await db.run('UPDATE guilds SET interest_rate = ? WHERE id = ?', percent, interaction.guildId);

        const embed = new EmbedBuilder()
            .setColor(0x00FF00)
            .setTitle('✅ Rendimento configurado')
            .setDescription(`Rendimento diário: **${percent}%**`)
            .setTimestamp();

        await interaction.reply({ embeds: [embed] });
    }

    async handleGuildApproveChannel(interaction) {
        if (!interaction.member.permissions.has('Administrator')) {
            return await interaction.reply({ content: 'Apenas administradores podem usar este comando.', flags: 64 });
        }

        const channel = interaction.options.getChannel('channel');
        await this.ensureGuild(interaction.guildId, interaction.guild.name);
        await db.run('UPDATE guilds SET approve_channel = ? WHERE id = ?', channel.id, interaction.guildId);

        const embed = new EmbedBuilder()
            .setColor(0x00FF00)
            .setTitle('✅ Canal de aprovação configurado')
            .setDescription(`Solicitações de empréstimo aparecerão em ${channel}`)
            .setTimestamp();

        await interaction.reply({ embeds: [embed] });
    }

    async handleGuildStaff(interaction) {
        if (!interaction.member.permissions.has('Administrator')) {
            return await interaction.reply({ content: 'Apenas administradores podem usar este comando.', flags: 64 });
        }

        const subcommand = interaction.options.getSubcommand();
        const role = interaction.options.getRole('role');

        if (subcommand === 'add') {
            await db.run(`
                INSERT OR IGNORE INTO staff_roles (guild_id, role_id)
                VALUES (?, ?)
            `, interaction.guildId, role.id);

            const embed = new EmbedBuilder()
                .setColor(0x00FF00)
                .setTitle('✅ Cargo adicionado')
                .setDescription(`${role} agora pode aprovar empréstimos`)
                .setTimestamp();

            await interaction.reply({ embeds: [embed] });
        } else {
            await db.run(`
                DELETE FROM staff_roles WHERE guild_id = ? AND role_id = ?
            `, interaction.guildId, role.id);

            const embed = new EmbedBuilder()
                .setColor(0x00FF00)
                .setTitle('✅ Cargo removido')
                .setDescription(`${role} não pode mais aprovar empréstimos`)
                .setTimestamp();

            await interaction.reply({ embeds: [embed] });
        }
    }

    async handleGuildNext(interaction) {
        if (!interaction.member.permissions.has('Administrator')) {
            return await interaction.reply({ content: 'Apenas administradores podem usar este comando.', flags: 64 });
        }

        await interaction.reply({ content: '⏳ Processando próximo ciclo diário...', flags: 64 });
        await this.loanManager.processDailyCycle();

        const embed = new EmbedBuilder()
            .setColor(0x00FF00)
            .setTitle('✅ Ciclo diário processado')
            .setDescription('Pagamentos de parcelas e rendimentos foram processados!')
            .setTimestamp();

        await interaction.followUp({ embeds: [embed] });
    }

    async handleGuildName(interaction) {
        if (!interaction.member.permissions.has('Administrator')) {
            return await interaction.reply({ content: 'Apenas administradores podem usar este comando.', flags: 64 });
        }

        const name = interaction.options.getString('name');
        await this.ensureGuild(interaction.guildId, interaction.guild.name);
        await db.run('UPDATE guilds SET name = ? WHERE id = ?', name, interaction.guildId);

        const embed = new EmbedBuilder()
            .setColor(0x00FF00)
            .setTitle('✅ Nome do banco atualizado')
            .setDescription(`Banco agora se chama **${name}**`)
            .setTimestamp();

        await interaction.reply({ embeds: [embed] });
    }

    async handleGuildDeposit(interaction) {
        if (!interaction.member.permissions.has('Administrator')) {
            return await interaction.reply({ content: 'Apenas administradores podem usar este comando.', flags: 64 });
        }

        const amount = interaction.options.getNumber('amount');
        const guild = await this.ensureGuild(interaction.guildId, interaction.guild.name);

        // Depositar na conta digital da própria guild
        const account = await this.ensureAccount(interaction.guildId, interaction.guildId);
        
        const currentBalance = parseFloat(formatAmount(account.balance));
        const newBalance = currentBalance + amount;
        
        await db.run(`
            UPDATE accounts SET balance = ? WHERE user_id = ? AND guild_id = ?
        `, storeAmount(newBalance), interaction.guildId, interaction.guildId);

        const embed = new EmbedBuilder()
            .setColor(0x00FF00)
            .setTitle('💰 Depósito realizado')
            .setDescription(`Foram depositados **${formatNumber(amount)} coin** no saldo digital do banco`)
            .addFields(
                { name: 'Novo saldo', value: `${formatNumber(newBalance)} coin`, inline: true }
            )
            .setTimestamp();

        await interaction.reply({ embeds: [embed] });
    }

    async handleGuildWithdraw(interaction) {
        if (!interaction.member.permissions.has('Administrator')) {
            return await interaction.reply({ content: 'Apenas administradores podem usar este comando.', flags: 64 });
        }

        const amount = interaction.options.getNumber('amount');
        const guild = await this.ensureGuild(interaction.guildId, interaction.guild.name);

        const account = await this.ensureAccount(interaction.guildId, interaction.guildId);
        const currentBalance = parseFloat(formatAmount(account.balance));

        if (currentBalance < amount) {
            return await interaction.reply({ content: '❌ Saldo insuficiente no banco!', flags: 64 });
        }

        const newBalance = currentBalance - amount;
        
        await db.run(`
            UPDATE accounts SET balance = ? WHERE user_id = ? AND guild_id = ?
        `, storeAmount(newBalance), interaction.guildId, interaction.guildId);

        const embed = new EmbedBuilder()
            .setColor(0x00FF00)
            .setTitle('💰 Saque realizado')
            .setDescription(`Foram sacados **${formatNumber(amount)} coin** do saldo digital do banco`)
            .addFields(
                { name: 'Novo saldo', value: `${formatNumber(newBalance)} coin`, inline: true }
            )
            .setTimestamp();

        await interaction.reply({ embeds: [embed] });
    }

    async handleScore(interaction) {
        if (!await this.isStaff(interaction.guildId, interaction.member)) {
            return await interaction.reply({ content: '❌ Você não tem permissão para usar este comando.', flags: 64 });
        }

        const userId = interaction.options.getString('user_id');
        
        const user = await db.get(`
            SELECT u.*, 
                   COUNT(DISTINCT a.guild_id) as total_accounts,
                   COUNT(DISTINCT l.id) as total_loans,
                   SUM(CASE WHEN l.status = 'active' THEN 1 ELSE 0 END) as active_loans,
                   SUM(CASE WHEN i.status = 'pending' AND i.due_date < ? THEN 1 ELSE 0 END) as overdue_installments
            FROM users u
            LEFT JOIN accounts a ON u.id = a.user_id
            LEFT JOIN loans l ON u.id = l.user_id
            LEFT JOIN installments i ON l.id = i.loan_id
            WHERE u.id = ?
            GROUP BY u.id
        `, Date.now(), userId);

        if (!user) {
            return await interaction.reply({ content: '❌ Usuário não encontrado!', flags: 64 });
        }

        const totalDeposits = parseFloat(formatAmount(user.total_deposits));
        const totalWithdrawals = parseFloat(formatAmount(user.total_withdrawals));

        const embed = new EmbedBuilder()
            .setColor(0x00FF00)
            .setTitle('📊 Score do usuário')
            .addFields(
                { name: 'Usuário', value: user.username || 'Desconhecido', inline: true },
                { name: 'ID', value: user.id, inline: true },
                { name: 'Score global', value: `${user.global_score}`, inline: true },
                { name: 'Contas em bancos', value: `${user.total_accounts}`, inline: true },
                { name: 'Total depositado', value: `${formatNumber(totalDeposits)} coin`, inline: true },
                { name: 'Total sacado', value: `${formatNumber(totalWithdrawals)} coin`, inline: true },
                { name: 'Parcelas restantes', value: `${user.active_loans || 0}`, inline: true },
                { name: 'Parcelas atrasadas', value: `${user.overdue_installments || 0}`, inline: true },
                { name: 'Membro desde', value: new Date(user.created_at).toLocaleDateString(), inline: true }
            )
            .setTimestamp();

        await interaction.reply({ embeds: [embed] });
    }

    async handleFinanceList(interaction) {
        const page = 1;
        const perPage = 10;
        
        const guilds = await db.all(`
            SELECT g.*, 
                   COUNT(DISTINCT a.user_id) as total_accounts,
                   SUM(CAST(a.balance AS INTEGER)) as total_balance
            FROM guilds g
            LEFT JOIN accounts a ON g.id = a.guild_id
            WHERE a.user_id != g.id
            GROUP BY g.id
            ORDER BY g.score DESC
            LIMIT ? OFFSET ?
        `, perPage, (page - 1) * perPage);

        const embed = new EmbedBuilder()
            .setColor(0x00FF00)
            .setTitle('🏦 Bancos disponíveis')
            .setDescription(guilds.map((g, i) => {
                const totalBalance = g.total_balance ? parseFloat(formatAmount(g.total_balance)) : 0;
                return `**${i + 1}.** ${g.name}\n` +
                       `└ Score: ${g.score} | Clientes: ${g.total_accounts} | Juros: ${g.tax_rate}% | Rendimento: ${g.interest_rate}% | Saldo: ${formatNumber(totalBalance)} coin`;
            }).join('\n\n') || 'Nenhum banco encontrado.')
            .setFooter({ text: `Página ${page}` })
            .setTimestamp();

        await interaction.reply({ embeds: [embed] });
    }

    async handleFinanceView(interaction) {
        const bankId = interaction.options.getString('banco');
        
        const guild = await db.get(`
            SELECT g.*, 
                   COUNT(DISTINCT a.user_id) as total_clients,
                   SUM(CASE WHEN a.user_id != g.id THEN CAST(a.balance AS INTEGER) ELSE 0 END) as total_balance,
                   (SELECT COUNT(*) FROM loans WHERE guild_id = g.id AND status = 'active') as active_loans,
                   (SELECT SUM(CAST(amount AS INTEGER)) FROM loans WHERE guild_id = g.id AND status = 'active') as total_lent,
                   (SELECT CAST(balance AS INTEGER) FROM accounts WHERE user_id = g.id AND guild_id = g.id) as bank_balance
            FROM guilds g
            LEFT JOIN accounts a ON g.id = a.guild_id
            WHERE g.id = ? OR g.name = ?
            GROUP BY g.id
        `, bankId, bankId);

        if (!guild) {
            return await interaction.reply({ content: '❌ Banco não encontrado!', flags: 64 });
        }

        const totalBalance = guild.total_balance ? parseFloat(formatAmount(guild.total_balance)) : 0;
        const bankBalance = guild.bank_balance ? parseFloat(formatAmount(guild.bank_balance)) : 0;
        const totalDeposits = parseFloat(formatAmount(guild.total_deposits));
        const totalWithdrawals = parseFloat(formatAmount(guild.total_withdrawals));
        const totalLent = guild.total_lent ? parseFloat(formatAmount(guild.total_lent)) : 0;
        const totalInvestmentPaid = parseFloat(formatAmount(guild.total_investment_paid));

        const embed = new EmbedBuilder()
            .setColor(0x00FF00)
            .setTitle(`🏦 ${guild.name}`)
            .addFields(
                { name: 'ID', value: guild.id, inline: true },
                { name: 'Score', value: `${guild.score}`, inline: true },
                { name: 'Clientes', value: `${guild.total_clients}`, inline: true },
                { name: 'Saldo dos clientes', value: `${formatNumber(totalBalance)} coin`, inline: true },
                { name: 'Saldo do banco', value: `${formatNumber(bankBalance)} coin`, inline: true },
                { name: 'Total depositado (real)', value: `${formatNumber(totalDeposits)} coin`, inline: true },
                { name: 'Total sacado (real)', value: `${formatNumber(totalWithdrawals)} coin`, inline: true },
                { name: 'Parcelas restantes', value: `${guild.active_loans || 0}`, inline: true },
                { name: 'Total emprestado', value: `${formatNumber(totalLent)} coin`, inline: true },
                { name: 'Juros', value: `${guild.tax_rate}%`, inline: true },
                { name: 'Rendimento', value: `${guild.interest_rate}%`, inline: true },
                { name: 'Já pago em rendimentos', value: `${formatNumber(totalInvestmentPaid)} coin`, inline: true },
                { name: 'Criado em', value: new Date(guild.created_at).toLocaleDateString(), inline: true }
            )
            .setTimestamp();

        await interaction.reply({ embeds: [embed] });
    }

    async handleFinanceDeposit(interaction) {
        const bankId = interaction.options.getString('banco');
        const amount = interaction.options.getNumber('valor');
        
        const user = await this.ensureUser(interaction.user.id, interaction.user.username);
        const guild = await db.get('SELECT * FROM guilds WHERE id = ? OR name = ?', bankId, bankId);

        if (!guild) {
            return await interaction.reply({ content: '❌ Banco não encontrado!', flags: 64 });
        }

        if (!guild.card_id) {
            return await interaction.reply({ content: '❌ Banco não tem card configurado!', flags: 64 });
        }

        if (!user.card_id) {
            return await interaction.reply({ content: '❌ Você precisa configurar seu card primeiro com /finance-card!', flags: 64 });
        }

        await interaction.reply({ content: '⏳ Processando depósito...', flags: 64 });

        try {
            // Transferir dinheiro real do card do usuário para o card da guild
            const payment = await api.pay(user.card_id, guild.card_id, amount);

            if (payment.success) {
                // Adicionar saldo digital na conta do usuário
                const account = await this.ensureAccount(user.id, guild.id);
                
                const currentBalance = parseFloat(formatAmount(account.balance));
                const newBalance = currentBalance + amount;
                
                await db.run(`
                    UPDATE accounts SET balance = ? WHERE user_id = ? AND guild_id = ?
                `, storeAmount(newBalance), user.id, guild.id);

                // Atualizar estatísticas
                const userTotalDeposits = parseFloat(formatAmount(user.total_deposits));
                await db.run(`
                    UPDATE users 
                    SET total_deposits = ?
                    WHERE id = ?
                `, storeAmount(userTotalDeposits + amount), user.id);

                const guildTotalDeposits = parseFloat(formatAmount(guild.total_deposits));
                await db.run(`
                    UPDATE guilds 
                    SET total_deposits = ?
                    WHERE id = ?
                `, storeAmount(guildTotalDeposits + amount), guild.id);

                // Bônus de score
                let scoreChange = CONFIG.SCORE_DEPOSIT_SMALL;
                if (amount >= CONFIG.LARGE_DEPOSIT) {
                    scoreChange = CONFIG.SCORE_DEPOSIT_LARGE;
                } else if (amount >= CONFIG.MEDIUM_DEPOSIT) {
                    scoreChange = CONFIG.SCORE_DEPOSIT_MEDIUM;
                }

                const now = Date.now();
                if (!user.last_deposit_bonus || (now - user.last_deposit_bonus) > 600000) {
                    await this.scoreManager.addChange(user.id, scoreChange, 
                        `Depósito de ${formatNumber(amount)} coin em ${guild.name}`);
                    await db.run('UPDATE users SET last_deposit_bonus = ? WHERE id = ?', now, user.id);
                }

                const embed = new EmbedBuilder()
                    .setColor(0x00FF00)
                    .setTitle('💰 Depósito realizado')
                    .setDescription(`Você depositou **${formatNumber(amount)} coin** em **${guild.name}**`)
                    .addFields(
                        { name: 'Transação', value: payment.txId, inline: true },
                        { name: 'Novo saldo digital', value: `${formatNumber(newBalance)} coin`, inline: true }
                    )
                    .setTimestamp();

                await interaction.followUp({ embeds: [embed] });

                if (guild.log_channel) {
                    try {
                        const logEmbed = new EmbedBuilder()
                            .setColor(0x00FF00)
                            .setTitle('💰 Depósito')
                            .setDescription(`${interaction.user.username} depositou **${formatNumber(amount)} coin**`)
                            .addFields(
                                { name: 'Usuário', value: interaction.user.username, inline: true },
                                { name: 'Valor', value: `${formatNumber(amount)} coin`, inline: true }
                            )
                            .setTimestamp();

                        const channel = await this.client.channels.fetch(guild.log_channel);
                        if (channel) await channel.send({ embeds: [logEmbed] });
                    } catch (error) {}
                }
            }
        } catch (error) {
            console.error('Deposit error:', error);
            
            const embed = new EmbedBuilder()
                .setColor(0xFF0000)
                .setTitle('❌ Falha no depósito')
                .setDescription(error.message === 'SAME_CARD' ?
                    'Erro: você não pode depositar para o mesmo cartão!' :
                    'Não foi possível realizar o depósito. Verifique seu saldo.')
                .setTimestamp();

            await interaction.followUp({ embeds: [embed] });
        }
    }

    async handleFinanceWithdraw(interaction) {
        const bankId = interaction.options.getString('banco');
        const amount = interaction.options.getNumber('valor');
        
        const user = await this.ensureUser(interaction.user.id, interaction.user.username);
        const guild = await db.get('SELECT * FROM guilds WHERE id = ? OR name = ?', bankId, bankId);

        if (!guild) {
            return await interaction.reply({ content: '❌ Banco não encontrado!', flags: 64 });
        }

        if (!guild.card_id) {
            return await interaction.reply({ content: '❌ Banco não tem card configurado!', flags: 64 });
        }

        if (!user.card_id) {
            return await interaction.reply({ content: '❌ Você precisa configurar seu card primeiro com /finance-card!', flags: 64 });
        }

        const account = await db.get(`
            SELECT * FROM accounts WHERE user_id = ? AND guild_id = ?
        `, user.id, guild.id);

        if (!account || parseFloat(formatAmount(account.balance)) < amount) {
            return await interaction.reply({ content: '❌ Saldo insuficiente neste banco!', flags: 64 });
        }

        await interaction.reply({ content: '⏳ Processando saque...', flags: 64 });

        try {
            const taxAmount = amount * CONFIG.WITHDRAW_TAX;
            const netAmount = amount - taxAmount;

            // Deduzir saldo digital do usuário
            const currentBalance = parseFloat(formatAmount(account.balance));
            const newBalance = currentBalance - amount;
            
            await db.run(`
                UPDATE accounts SET balance = ? WHERE user_id = ? AND guild_id = ?
            `, storeAmount(newBalance), user.id, guild.id);

            // Atualizar estatísticas
            const userTotalWithdrawals = parseFloat(formatAmount(user.total_withdrawals));
            await db.run(`
                UPDATE users 
                SET total_withdrawals = ?
                WHERE id = ?
            `, storeAmount(userTotalWithdrawals + amount), user.id);

            const guildTotalWithdrawals = parseFloat(formatAmount(guild.total_withdrawals));
            await db.run(`
                UPDATE guilds 
                SET total_withdrawals = ?
                WHERE id = ?
            `, storeAmount(guildTotalWithdrawals + amount), guild.id);

            // Transferir dinheiro real do card da guild para o card do usuário
            const payment = await api.pay(guild.card_id, user.card_id, netAmount);

            if (payment.success) {
                // Taxa vai para o card global
                if (taxAmount > 0 && guild.card_id !== CONFIG.GLOBAL_CARD) {
                    await api.pay(guild.card_id, CONFIG.GLOBAL_CARD, taxAmount).catch(() => {});
                }

                await this.scoreManager.addChange(user.id, CONFIG.SCORE_WITHDRAW, 
                    `Saque de ${formatNumber(amount)} coin de ${guild.name}`);

                const embed = new EmbedBuilder()
                    .setColor(0x00FF00)
                    .setTitle('💰 Saque realizado')
                    .setDescription(`Você sacou **${formatNumber(amount)} coin** de **${guild.name}**`)
                    .addFields(
                        { name: 'Valor bruto', value: `${formatNumber(amount)} coin`, inline: true },
                        { name: 'Taxa', value: `${formatNumber(taxAmount)} coin (${CONFIG.WITHDRAW_TAX * 100}%)`, inline: true },
                        { name: 'Valor líquido', value: `${formatNumber(netAmount)} coin`, inline: true },
                        { name: 'Transação', value: payment.txId, inline: true },
                        { name: 'Novo saldo digital', value: `${formatNumber(newBalance)} coin`, inline: true }
                    )
                    .setTimestamp();

                await interaction.followUp({ embeds: [embed] });

                if (guild.log_channel) {
                    try {
                        const logEmbed = new EmbedBuilder()
                            .setColor(0x00FF00)
                            .setTitle('💰 Saque')
                            .setDescription(`${interaction.user.username} sacou **${formatNumber(amount)} coin**`)
                            .addFields(
                                { name: 'Usuário', value: interaction.user.username, inline: true },
                                { name: 'Valor', value: `${formatNumber(amount)} coin`, inline: true },
                                { name: 'Taxa', value: `${formatNumber(taxAmount)} coin`, inline: true }
                            )
                            .setTimestamp();

                        const channel = await this.client.channels.fetch(guild.log_channel);
                        if (channel) await channel.send({ embeds: [logEmbed] });
                    } catch (error) {}
                }
            }
        } catch (error) {
            console.error('Withdraw error:', error);
            
            const embed = new EmbedBuilder()
                .setColor(0xFF0000)
                .setTitle('❌ Falha no saque')
                .setDescription(error.message === 'SAME_CARD' ?
                    'Erro: cartão de origem e destino são iguais!' :
                    'Não foi possível realizar o saque. Verifique o saldo do banco.')
                .setTimestamp();

            await interaction.followUp({ embeds: [embed] });
        }
    }

    async handleFinanceBalance(interaction) {
        const bankId = interaction.options.getString('banco');
        
        const user = await this.ensureUser(interaction.user.id, interaction.user.username);
        const guild = await db.get('SELECT * FROM guilds WHERE id = ? OR name = ?', bankId, bankId);

        if (!guild) {
            return await interaction.reply({ content: '❌ Banco não encontrado!', flags: 64 });
        }

        const account = await db.get(`
            SELECT * FROM accounts WHERE user_id = ? AND guild_id = ?
        `, user.id, guild.id);

        const balance = account ? parseFloat(formatAmount(account.balance)) : 0;

        const embed = new EmbedBuilder()
            .setColor(0x00FF00)
            .setTitle('💰 Saldo Digital')
            .setDescription(`Seu saldo em **${guild.name}**`)
            .addFields(
                { name: 'Saldo', value: `${formatNumber(balance)} coin`, inline: true },
                { name: 'Banco', value: guild.name, inline: true }
            )
            .setTimestamp();

        await interaction.reply({ embeds: [embed] });
    }

    async handleFinanceLoan(interaction) {
        const subcommand = interaction.options.getSubcommand();

        if (subcommand === 'take') {
            await this.handleLoanTake(interaction);
        } else if (subcommand === 'pay') {
            await this.handleLoanPay(interaction);
        }
    }

    async handleLoanTake(interaction) {
        const bankId = interaction.options.getString('banco');
        const amount = interaction.options.getNumber('valor');
        const installments = interaction.options.getInteger('parcelas');

        const user = await this.ensureUser(interaction.user.id, interaction.user.username);
        const guild = await db.get('SELECT * FROM guilds WHERE id = ? OR name = ?', bankId, bankId);

        if (!guild) {
            return await interaction.reply({ content: '❌ Banco não encontrado!', flags: 64 });
        }

        if (!guild.approve_channel) {
            return await interaction.reply({ content: '❌ Este banco não tem canal de aprovação configurado!', flags: 64 });
        }

        // Verificar se já tem um empréstimo ativo
        const activeLoan = await db.get(`
            SELECT * FROM loans 
            WHERE user_id = ? AND guild_id = ? AND status = 'active'
        `, user.id, guild.id);

        if (activeLoan) {
            return await interaction.reply({ 
                content: '❌ Você já tem um empréstimo ativo neste banco!', 
                flags: 64 
            });
        }

        // Verificar se tem um empréstimo pendente
        const pendingLoan = await db.get(`
            SELECT * FROM loans 
            WHERE user_id = ? AND guild_id = ? AND status = 'pending'
        `, user.id, guild.id);

        if (pendingLoan) {
            return await interaction.reply({ 
                content: '❌ Você já tem uma solicitação pendente neste banco!', 
                flags: 64 
            });
        }

        // Verificar saldo do banco
        const bankAccount = await db.get(`
            SELECT * FROM accounts WHERE user_id = ? AND guild_id = ?
        `, guild.id, guild.id);

        const bankBalance = bankAccount ? parseFloat(formatAmount(bankAccount.balance)) : 0;
        if (bankBalance < amount) {
            return await interaction.reply({ 
                content: '❌ Banco não tem saldo suficiente para este empréstimo!', 
                flags: 64 
            });
        }

        const userStats = await db.get(`
            SELECT 
                COUNT(DISTINCT a.guild_id) as total_accounts,
                SUM(CAST(a.balance AS INTEGER)) as total_balance,
                COUNT(DISTINCT l.id) as total_loans,
                SUM(CASE WHEN l.status = 'active' THEN 1 ELSE 0 END) as active_loans,
                SUM(CAST(l.total_amount AS INTEGER)) as total_debt,
                u.global_score,
                u.total_deposits,
                u.total_withdrawals
            FROM users u
            LEFT JOIN accounts a ON u.id = a.user_id
            LEFT JOIN loans l ON u.id = l.user_id
            WHERE u.id = ?
            GROUP BY u.id
        `, user.id);

        const totalBalance = userStats.total_balance ? parseFloat(formatAmount(userStats.total_balance)) : 0;
        const totalDeposits = parseFloat(formatAmount(userStats.total_deposits));
        const totalWithdrawals = parseFloat(formatAmount(userStats.total_withdrawals));
        const totalDebt = userStats.total_debt ? parseFloat(formatAmount(userStats.total_debt)) : 0;

        const totalWithInterest = amount * (1 + (guild.tax_rate / 100));
        const installmentValue = totalWithInterest / installments;

        const embed = new EmbedBuilder()
            .setColor(0xFFFF00)
            .setTitle('📝 Solicitação de empréstimo')
            .addFields(
                { name: 'Usuário', value: interaction.user.username, inline: true },
                { name: 'ID', value: interaction.user.id, inline: true },
                { name: 'Score global', value: `${userStats.global_score}`, inline: true },
                { name: 'Contas em bancos', value: `${userStats.total_accounts || 0}`, inline: true },
                { name: 'Saldo total', value: `${formatNumber(totalBalance)} coin`, inline: true },
                { name: 'Total depositado', value: `${formatNumber(totalDeposits)} coin`, inline: true },
                { name: 'Total sacado', value: `${formatNumber(totalWithdrawals)} coin`, inline: true },
                { name: 'Parcelas restantes', value: `${userStats.active_loans || 0}`, inline: true },
                { name: 'Dívida total', value: `${formatNumber(totalDebt)} coin`, inline: true },
                { name: '\u200B', value: '\u200B', inline: true },
                { name: 'Valor solicitado', value: `${formatNumber(amount)} coin`, inline: true },
                { name: 'Parcelas', value: `${installments}x`, inline: true },
                { name: 'Juros do banco', value: `${guild.tax_rate}%`, inline: true },
                { name: 'Total com juros', value: `${formatNumber(totalWithInterest)} coin`, inline: true },
                { name: 'Valor da parcela', value: `${formatNumber(installmentValue)} coin`, inline: true }
            )
            .setFooter({ text: `ID: ${interaction.user.id}` })
            .setTimestamp();

        const loanId = generateId();

        const row = new ActionRowBuilder()
            .addComponents(
                new ButtonBuilder()
                    .setCustomId(`approve_loan:${loanId}`)
                    .setLabel('✅ Aprovar')
                    .setStyle(ButtonStyle.Success),
                new ButtonBuilder()
                    .setCustomId(`reject_loan:${loanId}`)
                    .setLabel('❌ Reprovar')
                    .setStyle(ButtonStyle.Danger)
            );

        await db.run(`
            INSERT INTO loans (id, user_id, guild_id, amount, total_amount, installments, 
                             installment_value, interest_rate, status, created_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `, loanId, user.id, guild.id, storeAmount(amount), storeAmount(totalWithInterest), 
           installments, storeAmount(installmentValue), guild.tax_rate, 'pending', Date.now(),
           JSON.stringify({ username: interaction.user.username }));

        try {
            const channel = await this.client.channels.fetch(guild.approve_channel);
            await channel.send({ embeds: [embed], components: [row] });

            const successEmbed = new EmbedBuilder()
                .setColor(0x00FF00)
                .setTitle('✅ Solicitação enviada')
                .setDescription(`Sua solicitação foi enviada para **${guild.name}**`)
                .setTimestamp();

            await interaction.reply({ embeds: [successEmbed] });
        } catch (error) {
            await interaction.reply({ 
                content: '❌ Erro ao enviar solicitação.', 
                flags: 64 
            });
        }
    }

    async handleLoanPay(interaction) {
        const bankId = interaction.options.getString('banco');
        const value = interaction.options.getString('valor');

        const user = await this.ensureUser(interaction.user.id, interaction.user.username);
        const guild = await db.get('SELECT * FROM guilds WHERE id = ? OR name = ?', bankId, bankId);

        if (!guild) {
            return await interaction.reply({ content: '❌ Banco não encontrado!', flags: 64 });
        }

        const account = await db.get(`
            SELECT * FROM accounts WHERE user_id = ? AND guild_id = ?
        `, user.id, guild.id);

        if (!account || parseFloat(formatAmount(account.balance)) <= 0) {
            return await interaction.reply({ content: '❌ Você não tem saldo neste banco!', flags: 64 });
        }

        const loan = await db.get(`
            SELECT l.*, 
                   COUNT(i.id) as total_installments,
                   SUM(CASE WHEN i.status = 'pending' THEN 1 ELSE 0 END) as pending_installments,
                   SUM(CASE WHEN i.status = 'pending' THEN CAST(i.amount AS INTEGER) ELSE 0 END) as pending_amount
            FROM loans l
            LEFT JOIN installments i ON l.id = i.loan_id
            WHERE l.user_id = ? AND l.guild_id = ? AND l.status = 'active'
            GROUP BY l.id
        `, user.id, guild.id);

        if (!loan) {
            return await interaction.reply({ content: '❌ Você não tem empréstimo ativo neste banco!', flags: 64 });
        }

        const balance = parseFloat(formatAmount(account.balance));
        const pendingAmount = parseFloat(parseLoanValue(loan.pending_amount || 0));
        const installmentValue = parseFloat(parseLoanValue(loan.installment_value));

        let amountToPay = 0;
        let installmentsToPay = 0;

        if (value.toLowerCase() === 'max') {
            amountToPay = Math.min(balance, pendingAmount);
            installmentsToPay = Math.floor(amountToPay / installmentValue);
            if (installmentsToPay === 0) {
                return await interaction.reply({ 
                    content: '❌ Saldo insuficiente para pagar uma parcela completa!', 
                    flags: 64 
                });
            }
            amountToPay = installmentsToPay * installmentValue;
        } else if (value.endsWith('x')) {
            installmentsToPay = parseInt(value);
            if (isNaN(installmentsToPay) || installmentsToPay <= 0) {
                return await interaction.reply({ content: '❌ Número de parcelas inválido!', flags: 64 });
            }
            amountToPay = installmentValue * installmentsToPay;
        } else {
            return await interaction.reply({ 
                content: '❌ Use "max" ou número de parcelas (ex: 5x)', 
                flags: 64 
            });
        }

        if (amountToPay <= 0 || amountToPay > balance || amountToPay > pendingAmount) {
            return await interaction.reply({ 
                content: '❌ Saldo insuficiente ou valor excede dívida!', 
                flags: 64 
            });
        }

        await interaction.reply({ content: '⏳ Processando pagamento...', flags: 64 });

        try {
            const installments = await db.all(`
                SELECT * FROM installments 
                WHERE loan_id = ? AND status = 'pending'
                ORDER BY due_date ASC
                LIMIT ?
            `, loan.id, installmentsToPay);

            let totalPaid = 0;
            for (const inst of installments) {
                totalPaid += parseFloat(parseLoanValue(inst.amount));
            }

            // Deduzir do saldo do usuário
            const newBalance = balance - totalPaid;
            await db.run(`
                UPDATE accounts SET balance = ? WHERE user_id = ? AND guild_id = ?
            `, storeAmount(newBalance), user.id, guild.id);

            // Adicionar ao saldo do banco
            const bankAccount = await db.get(`
                SELECT * FROM accounts WHERE user_id = ? AND guild_id = ?
            `, guild.id, guild.id);
            
            const bankBalance = parseFloat(formatAmount(bankAccount.balance));
            await db.run(`
                UPDATE accounts SET balance = ? WHERE user_id = ? AND guild_id = ?
            `, storeAmount(bankBalance + totalPaid), guild.id, guild.id);

            // Marcar parcelas como pagas
            for (const inst of installments) {
                await db.run(`
                    UPDATE installments 
                    SET status = 'paid', paid_date = ?
                    WHERE id = ?
                `, Date.now(), inst.id);
            }

            await db.run(`
                UPDATE loans 
                SET paid_installments = paid_installments + ?,
                    last_payment = ?
                WHERE id = ?
            `, installments.length, Date.now(), loan.id);

            await this.scoreManager.addChange(user.id, CONFIG.SCORE_LOAN_PAID * installments.length, 
                `Pagamento de ${installments.length} parcelas em ${guild.name}`);

            // Verificar se o empréstimo foi concluído
            const updatedLoan = await db.get('SELECT * FROM loans WHERE id = ?', loan.id);
            if (updatedLoan && updatedLoan.paid_installments >= updatedLoan.installments) {
                await this.loanManager.completeLoan(loan.id);
            }

            const embed = new EmbedBuilder()
                .setColor(0x00FF00)
                .setTitle('✅ Pagamento realizado')
                .setDescription(`Você pagou **${installments.length} parcelas** em **${guild.name}**`)
                .addFields(
                    { name: 'Valor pago', value: `${formatNumber(totalPaid)} coin`, inline: true },
                    { name: 'Parcelas pagas', value: `${installments.length}`, inline: true },
                    { name: 'Parcelas restantes', value: `${loan.pending_installments - installments.length}`, inline: true },
                    { name: 'Novo saldo', value: `${formatNumber(newBalance)} coin`, inline: true }
                )
                .setTimestamp();

            await interaction.followUp({ embeds: [embed] });

            if (guild.log_channel) {
                try {
                    const logEmbed = new EmbedBuilder()
                        .setColor(0x00FF00)
                        .setTitle('💰 Pagamento de empréstimo')
                        .setDescription(`${interaction.user.username} pagou ${installments.length} parcelas`)
                        .addFields(
                            { name: 'Valor', value: `${formatNumber(totalPaid)} coin`, inline: true },
                            { name: 'Parcelas', value: `${installments.length}`, inline: true }
                        )
                        .setTimestamp();

                    const channel = await this.client.channels.fetch(guild.log_channel);
                    if (channel) await channel.send({ embeds: [logEmbed] });
                } catch (error) {}
            }
        } catch (error) {
            console.error('Loan payment error:', error);
            
            const embed = new EmbedBuilder()
                .setColor(0xFF0000)
                .setTitle('❌ Falha no pagamento')
                .setDescription('Ocorreu um erro ao processar o pagamento.')
                .setTimestamp();

            await interaction.followUp({ embeds: [embed] });
        }
    }

    async handleFinanceScore(interaction) {
        const user = await this.ensureUser(interaction.user.id, interaction.user.username);

        const stats = await db.get(`
            SELECT 
                u.global_score,
                u.total_deposits,
                u.total_withdrawals,
                u.total_loans,
                u.total_loans_paid,
                u.total_interest_paid,
                u.accounts_count,
                u.created_at,
                COUNT(DISTINCT l.id) as total_loans_count,
                SUM(CASE WHEN l.status = 'active' THEN 1 ELSE 0 END) as active_loans,
                SUM(CASE WHEN i.status = 'pending' AND i.due_date < ? THEN 1 ELSE 0 END) as overdue_installments,
                SUM(CAST(l.total_amount AS INTEGER)) as total_debt
            FROM users u
            LEFT JOIN loans l ON u.id = l.user_id
            LEFT JOIN installments i ON l.id = i.loan_id
            WHERE u.id = ?
            GROUP BY u.id
        `, Date.now(), user.id);

        const accounts = await db.all(`
            SELECT a.*, g.name as guild_name, g.interest_rate, g.tax_rate
            FROM accounts a
            JOIN guilds g ON a.guild_id = g.id
            WHERE a.user_id = ?
        `, user.id);

        const totalDeposits = parseFloat(formatAmount(stats.total_deposits));
        const totalWithdrawals = parseFloat(formatAmount(stats.total_withdrawals));
        const totalDebt = stats.total_debt ? parseFloat(parseLoanValue(stats.total_debt)) : 0;
        const totalLoansPaid = parseFloat(formatAmount(stats.total_loans_paid));
        const totalInterestPaid = parseFloat(formatAmount(stats.total_interest_paid));

        const embed = new EmbedBuilder()
            .setColor(0x00FF00)
            .setTitle('📊 Seus dados financeiros')
            .addFields(
                { name: 'Score global', value: `${stats.global_score}`, inline: true },
                { name: 'Contas em bancos', value: `${stats.accounts_count}`, inline: true },
                { name: 'Total depositado (real)', value: `${formatNumber(totalDeposits)} coin`, inline: true },
                { name: 'Total sacado (real)', value: `${formatNumber(totalWithdrawals)} coin`, inline: true },
                { name: 'Parcelas restantes', value: `${stats.active_loans || 0}`, inline: true },
                { name: 'Dívida total', value: `${formatNumber(totalDebt)} coin`, inline: true },
                { name: 'Parcelas atrasadas', value: `${stats.overdue_installments || 0}`, inline: true },
                { name: 'Total de empréstimos', value: `${stats.total_loans_count || 0}`, inline: true },
                { name: 'Total já pago', value: `${formatNumber(totalLoansPaid)} coin`, inline: true },
                { name: 'Juros pagos', value: `${formatNumber(totalInterestPaid)} coin`, inline: true },
                { name: 'Membro desde', value: new Date(stats.created_at).toLocaleDateString(), inline: true }
            )
            .setTimestamp();

        if (accounts.length > 0) {
            let accountsText = '';
            for (const acc of accounts) {
                const balance = parseFloat(formatAmount(acc.balance));
                accountsText += `**${acc.guild_name}**: ${formatNumber(balance)} coin\n`;
            }
            embed.addFields({ name: 'Saldos digitais por banco', value: accountsText });
        }

        await interaction.reply({ embeds: [embed] });
    }

    async handleFinanceCard(interaction) {
        const cardId = interaction.options.getString('card_id');

        await this.ensureUser(interaction.user.id, interaction.user.username);

        const check = await api.checkBalance(cardId);
        
        if (!check || !check.success) {
            return await interaction.reply({ 
                content: '❌ Card inválido ou não encontrado na rede!', 
                flags: 64 
            });
        }

        await db.run(`
            UPDATE users SET card_id = ? WHERE id = ?
        `, cardId, interaction.user.id);

        const embed = new EmbedBuilder()
            .setColor(0x00FF00)
            .setTitle('✅ Card configurado')
            .setDescription('Seu card foi configurado com sucesso!')
            .addFields(
                { name: 'Card ID', value: "", inline: true },
                { name: 'Saldo', value: `${formatNumber(check.coins || 0)} coin`, inline: true }
            )
            .setTimestamp();

        await interaction.reply({ embeds: [embed] });
    }

    async handleLoanApproval(interaction, loanId) {
        if (!await this.isStaff(interaction.guildId, interaction.member)) {
            return await interaction.reply({ 
                content: '❌ Você não tem permissão para aprovar empréstimos.', 
                flags: 64 
            });
        }

        const loan = await db.get(`
            SELECT l.*, u.username, u.card_id, g.name as guild_name, g.card_id as guild_card_id
            FROM loans l
            JOIN users u ON l.user_id = u.id
            JOIN guilds g ON l.guild_id = g.id
            WHERE l.id = ?
        `, loanId);

        if (!loan) {
            return await interaction.reply({ 
                content: '❌ Empréstimo não encontrado!', 
                flags: 64 
            });
        }

        if (loan.status !== 'pending') {
            return await interaction.reply({ 
                content: '❌ Este empréstimo já foi processado!', 
                flags: 64 
            });
        }

        try {
            const amount = parseFloat(parseLoanValue(loan.amount));
            
            // Verificar saldo do banco
            const bankAccount = await db.get(`
                SELECT * FROM accounts WHERE user_id = ? AND guild_id = ?
            `, loan.guild_id, loan.guild_id);

            const bankBalance = bankAccount ? parseFloat(formatAmount(bankAccount.balance)) : 0;
            
            if (bankBalance < amount) {
                const embed = new EmbedBuilder()
                    .setColor(0xFF0000)
                    .setTitle('❌ Falha na aprovação')
                    .setDescription('Banco não tem saldo suficiente para este empréstimo!')
                    .setTimestamp();

                await interaction.update({ embeds: [embed], components: [] });
                return;
            }

            // Deduzir do saldo do banco
            const newBankBalance = bankBalance - amount;
            await db.run(`
                UPDATE accounts SET balance = ? WHERE user_id = ? AND guild_id = ?
            `, storeAmount(newBankBalance), loan.guild_id, loan.guild_id);

            // Adicionar ao saldo do usuário
            const userAccount = await this.ensureAccount(loan.user_id, loan.guild_id);
            const userBalance = parseFloat(formatAmount(userAccount.balance));
            await db.run(`
                UPDATE accounts SET balance = ? WHERE user_id = ? AND guild_id = ?
            `, storeAmount(userBalance + amount), loan.user_id, loan.guild_id);

            // Atualizar status do empréstimo
            await db.run(`
                UPDATE loans 
                SET status = 'active', approved_at = ?, approved_by = ?
                WHERE id = ?
            `, Date.now(), interaction.user.id, loan.id);

            // Criar parcelas
            const installmentValue = parseFloat(parseLoanValue(loan.installment_value));
            const now = Date.now();

            for (let i = 1; i <= loan.installments; i++) {
                const dueDate = now + (i * 86400000);
                await db.run(`
                    INSERT INTO installments (id, loan_id, number, amount, due_date, status)
                    VALUES (?, ?, ?, ?, ?, 'pending')
                `, generateId(), loan.id, i, storeAmount(installmentValue), dueDate);
            }

            // Atualizar estatísticas da guild
            const guildTotalLoans = parseFloat(formatAmount(loan.total_loans_given || 0));
            const guildTotalInterest = parseFloat(formatAmount(loan.total_interest_earned || 0));
            
            await db.run(`
                UPDATE guilds 
                SET total_loans_given = ?,
                    total_interest_earned = ?
                WHERE id = ?
            `, storeAmount(guildTotalLoans + amount), 
               storeAmount(guildTotalInterest + (parseFloat(parseLoanValue(loan.total_amount)) - amount)), 
               loan.guild_id);

            // Atualizar estatísticas do usuário
            const userTotalLoans = parseFloat(formatAmount(loan.total_loans || 0));
            await db.run(`
                UPDATE users 
                SET total_loans = ?
                WHERE id = ?
            `, storeAmount(userTotalLoans + amount), loan.user_id);

            const embed = new EmbedBuilder()
                .setColor(0x00FF00)
                .setTitle('✅ Empréstimo aprovado')
                .setDescription(`Empréstimo para **${loan.username}** foi aprovado!`)
                .addFields(
                    { name: 'Valor', value: `${formatNumber(amount)} coin`, inline: true },
                    { name: 'Parcelas', value: `${loan.installments}x`, inline: true },
                    { name: 'Aprovado por', value: interaction.user.username, inline: true }
                )
                .setTimestamp();

            await interaction.update({ embeds: [embed], components: [] });

            const userEmbed = new EmbedBuilder()
                .setColor(0x00FF00)
                .setTitle('✅ Empréstimo aprovado')
                .setDescription(`Seu empréstimo de **${formatNumber(amount)} coin** em **${loan.guild_name}** foi aprovado!`)
                .addFields(
                    { name: 'Valor creditado', value: `${formatNumber(amount)} coin`, inline: true },
                    { name: 'Parcelas', value: `${loan.installments}x`, inline: true }
                )
                .setTimestamp();

            await this.dmManager.send(loan.user_id, userEmbed);

        } catch (error) {
            console.error('Loan approval error:', error);
            
            const embed = new EmbedBuilder()
                .setColor(0xFF0000)
                .setTitle('❌ Falha na aprovação')
                .setDescription('Erro ao processar a aprovação do empréstimo.')
                .setTimestamp();

            try {
                await interaction.update({ embeds: [embed], components: [] });
            } catch (updateError) {
                console.error('Error updating interaction:', updateError);
                try {
                    await interaction.followUp({ embeds: [embed], flags: 64 });
                } catch (followUpError) {
                    console.error('Error sending followup:', followUpError);
                }
            }
        }
    }

    async handleLoanRejection(interaction, loanId) {
        if (!await this.isStaff(interaction.guildId, interaction.member)) {
            return await interaction.reply({ 
                content: '❌ Você não tem permissão para rejeitar empréstimos.', 
                flags: 64 
            });
        }

        const loan = await db.get(`
            SELECT l.*, u.username, g.name as guild_name
            FROM loans l
            JOIN users u ON l.user_id = u.id
            JOIN guilds g ON l.guild_id = g.id
            WHERE l.id = ?
        `, loanId);

        if (!loan) {
            return await interaction.reply({ 
                content: '❌ Empréstimo não encontrado!', 
                flags: 64 
            });
        }

        if (loan.status !== 'pending') {
            return await interaction.reply({ 
                content: '❌ Este empréstimo já foi processado!', 
                flags: 64 
            });
        }

        try {
            // Atualizar status do empréstimo para rejeitado
            await db.run(`
                UPDATE loans SET status = 'rejected' WHERE id = ?
            `, loan.id);

            const embed = new EmbedBuilder()
                .setColor(0xFF0000)
                .setTitle('❌ Empréstimo rejeitado')
                .setDescription(`Empréstimo para **${loan.username}** foi rejeitado.`)
                .addFields(
                    { name: 'Rejeitado por', value: interaction.user.username, inline: true }
                )
                .setTimestamp();

            await interaction.update({ embeds: [embed], components: [] });

            const userEmbed = new EmbedBuilder()
                .setColor(0xFF0000)
                .setTitle('❌ Empréstimo rejeitado')
                .setDescription(`Seu empréstimo em **${loan.guild_name}** foi rejeitado.`)
                .setTimestamp();

            await this.dmManager.send(loan.user_id, userEmbed);

        } catch (error) {
            console.error('Loan rejection error:', error);
            
            const embed = new EmbedBuilder()
                .setColor(0xFF0000)
                .setTitle('❌ Falha na rejeição')
                .setDescription('Erro ao processar a rejeição do empréstimo.')
                .setTimestamp();

            try {
                await interaction.update({ embeds: [embed], components: [] });
            } catch (updateError) {
                console.error('Error updating interaction:', updateError);
                try {
                    await interaction.followUp({ embeds: [embed], flags: 64 });
                } catch (followUpError) {
                    console.error('Error sending followup:', followUpError);
                }
            }
        }
    }

    async start() {
        await initializeDatabase();
        await this.client.login(CONFIG.TOKEN);
    }
}

// ===================== INICIAR BOT =====================
const bot = new FinanceBot();
bot.start().catch(console.error);

// ===================== CLEANUP =====================
process.on('SIGINT', async () => {
    console.log('\n🛑 Desligando...');
    api.queue.stop();
    scoreManager.queue.stop();
    if (dmManager) dmManager.queue.stop();
    await db?.close();
    process.exit(0);
});

process.on('unhandledRejection', (error) => {
    console.error('Unhandled Rejection:', error);
});

process.on('uncaughtException', (error) => {
    console.error('Uncaught Exception:', error);
});
