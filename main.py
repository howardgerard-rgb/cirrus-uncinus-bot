# ========================
# cirrus uncinus - light cloud bot
# Improved Version with Database, Caching, and Better Architecture
# ========================

import discord
from discord.ext import tasks
from discord import app_commands
import aiohttp
import os
import json
import logging
from dotenv import load_dotenv
from flask import Flask
from threading import Thread
from datetime import datetime, timedelta
from timezonefinder import TimezoneFinder
import pytz
import math
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ========================
# LOGGING CONFIGURATION
# ========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('CirrusUncinus')

# ========================
# CONFIGURATION
# ========================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
NASA_API_KEY = os.getenv("NASA_API_KEY")

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN not found in environment variables")

# ========================
# SCIENTIFIC CONSTANTS
# ========================
VAPOR_PRESSURE_BASE = 6.112  # hPa
VAPOR_PRESSURE_COEFF = 17.67
VAPOR_PRESSURE_TEMP_BASE = 243.5  # °C
GAS_CONSTANT_DRY_AIR = 287.05  # J/(kg·K)
STANDARD_PRESSURE = 1013.25  # hPa
KELVIN_OFFSET = 273.15

# Cache settings
CACHE_TTL_MINUTES = 5
WEATHER_CACHE_TTL = timedelta(minutes=CACHE_TTL_MINUTES)
NASA_CACHE_TTL = timedelta(hours=12)

# Rate limiting
COMMAND_COOLDOWN_SECONDS = 30

# Database file
DB_FILE = "user_settings.json"

# ========================
# BOT SETUP
# ========================
intents = discord.Intents.default()
intents.message_content = True
bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)

# ========================
# DATA STRUCTURES & CACHE
# ========================
user_settings: dict[int, dict] = {}
weather_cache: dict[str, tuple[dict, datetime]] = {}
nasa_cache: tuple[tuple[str, str, str], datetime] | None = None

# ========================
# DATABASE FUNCTIONS
# ========================

def load_user_settings() -> None:
    """Load user settings from JSON file"""
    global user_settings
    try:
        if os.path.exists(DB_FILE):
            with open(DB_FILE, 'r') as f:
                # Convert string keys back to int
                data = json.load(f)
                user_settings = {int(k): v for k, v in data.items()}
            logger.info(f"Loaded {len(user_settings)} user settings from database")
        else:
            user_settings = {}
            logger.info("No existing database found, starting fresh")
    except Exception as e:
        logger.error(f"Error loading user settings: {e}")
        user_settings = {}

def save_user_settings() -> None:
    """Save user settings to JSON file"""
    try:
        # Convert int keys to string for JSON
        data = {str(k): v for k, v in user_settings.items()}
        with open(DB_FILE, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Saved {len(user_settings)} user settings to database")
    except Exception as e:
        logger.error(f"Error saving user settings: {e}")

# ========================
# METEOROLOGICAL CALCULATIONS
# ========================

def calculate_dewpoint(temp_c: float, humidity: float) -> float:
    """
    Calculate dewpoint using Magnus-Tetens formula
    Returns: Dewpoint in Celsius
    """
    try:
        a = VAPOR_PRESSURE_COEFF
        b = VAPOR_PRESSURE_TEMP_BASE
        alpha = ((a * temp_c) / (b + temp_c)) + math.log(humidity / 100.0)
        dewpoint = (b * alpha) / (a - alpha)
        return round(dewpoint, 2)
    except Exception as e:
        logger.error(f"Dewpoint calculation error: {e}")
        return 0.0

def calculate_heat_index(temp_c: float, humidity: float) -> float:
    """
    Calculate heat index (apparent temperature)
    Uses Rothfusz regression (NWS algorithm)
    Returns: Heat index in Celsius
    """
    try:
        temp_f = (temp_c * 9/5) + 32
        
        if temp_f < 80:
            return temp_c
        
        hi = -42.379 + 2.04901523 * temp_f + 10.14333127 * humidity
        hi -= 0.22475541 * temp_f * humidity
        hi -= 0.00683783 * temp_f * temp_f
        hi -= 0.05481717 * humidity * humidity
        hi += 0.00122874 * temp_f * temp_f * humidity
        hi += 0.00085282 * temp_f * humidity * humidity
        hi -= 0.00000199 * temp_f * temp_f * humidity * humidity
        
        hi_c = (hi - 32) * 5/9
        return round(hi_c, 2)
    except Exception as e:
        logger.error(f"Heat index calculation error: {e}")
        return temp_c

def calculate_air_density(temp_c: float, pressure_hpa: float, humidity: float) -> float:
    """
    Calculate air density using ideal gas law with humidity correction
    Returns: Density in kg/m³
    """
    try:
        temp_k = temp_c + KELVIN_OFFSET
        
        es = VAPOR_PRESSURE_BASE * math.exp((VAPOR_PRESSURE_COEFF * temp_c) / (temp_c + VAPOR_PRESSURE_TEMP_BASE))
        e = (humidity / 100.0) * es
        pd = (pressure_hpa * 100) - (e * 100)
        density = (pd / (GAS_CONSTANT_DRY_AIR * temp_k))
        
        return round(density, 4)
    except Exception as e:
        logger.error(f"Air density calculation error: {e}")
        return 1.225

def classify_clouds_scientific(cloud_pct: float) -> tuple[str, str, int]:
    """
    Classify clouds using WMO okta scale and scientific nomenclature
    Returns: (classification, description, oktas)
    """
    oktas = round(cloud_pct / 12.5)
    
    classifications = {
        0: ("SKC", "Sky Clear - No cloud coverage detected", 0),
        1: ("FEW", "Few clouds - Cumulus humilis or fractus", 1),
        2: ("FEW", "Few clouds - Isolated cumulus development", 2),
        3: ("SCT", "Scattered - Cumulus mediocris formation", 3),
        4: ("SCT", "Scattered - Multiple cumulus or stratocumulus", 4),
        5: ("BKN", "Broken - Extensive stratocumulus or altocumulus", 5),
        6: ("BKN", "Broken - Altostratus or nimbostratus forming", 6),
        7: ("BKN", "Broken - Pre-overcast conditions", 7),
        8: ("OVC", "Overcast - Complete cloud coverage (nimbostratus/stratus)", 8),
    }
    
    return classifications.get(oktas, classifications[8])

def calculate_cloud_base_height(temp_c: float, dewpoint: float) -> int:
    """
    Estimate cloud base height using temperature-dewpoint spread
    Uses simplified Hennig formula: height = 125 * (T - Td) meters
    Returns: Cloud base height in meters
    """
    try:
        spread = temp_c - dewpoint
        height = 125 * spread
        return int(height)
    except Exception:
        return 0

def get_visibility_category(vis_m: int) -> str:
    """Categorize visibility based on WMO standards"""
    if vis_m >= 10000:
        return "Excellent (>10km)"
    elif vis_m >= 5000:
        return "Good (5-10km)"
    elif vis_m >= 2000:
        return "Moderate (2-5km)"
    elif vis_m >= 1000:
        return "Poor (1-2km)"
    else:
        return f"Very Poor ({vis_m}m)"

def convert_temperature(temp_c: float, unit: str) -> tuple[float, str]:
    """Convert temperature to specified unit"""
    if unit == "fahrenheit":
        return round((temp_c * 9/5) + 32, 1), "°F"
    elif unit == "kelvin":
        return round(temp_c + KELVIN_OFFSET, 1), "K"
    else:  # celsius
        return round(temp_c, 1), "°C"

# ========================
# API FUNCTIONS WITH CACHING
# ========================

async def get_weather(city: str) -> dict | None:
    """Fetch comprehensive meteorological data from OpenWeather API with caching"""
    cache_key = city.lower()
    now = datetime.now()
    
    # Check cache
    if cache_key in weather_cache:
        cached_data, timestamp = weather_cache[cache_key]
        if now - timestamp < WEATHER_CACHE_TTL:
            logger.debug(f"Cache hit for {city}")
            return cached_data
    
    # Fetch new data
    try:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={OPENWEATHER_API_KEY}&units=metric"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                response.raise_for_status()
                data = await response.json()
        
        result = {
            "temp": data["main"]["temp"],
            "feels_like": data["main"]["feels_like"],
            "pressure": data["main"]["pressure"],
            "humidity": data["main"]["humidity"],
            "clouds": data["clouds"]["all"],
            "wind_speed": data["wind"]["speed"],
            "wind_deg": data["wind"].get("deg", 0),
            "visibility": data.get("visibility", 10000),
            "description": data["weather"][0]["description"],
            "condition_id": data["weather"][0]["id"],
            "lat": data["coord"]["lat"],
            "lon": data["coord"]["lon"],
            "city_name": data["name"],
            "country": data["sys"]["country"],
        }
        
        weather_cache[cache_key] = (result, now)
        logger.info(f"Fetched weather data for {city}")
        return result
        
    except Exception as e:
        logger.error(f"Weather API error for {city}: {e}")
        return None

def get_timezone_from_coords(lat: float, lon: float) -> str:
    """Determine timezone from geographic coordinates"""
    try:
        tf = TimezoneFinder()
        tz_str = tf.timezone_at(lat=lat, lng=lon)
        return tz_str if tz_str else "UTC"
    except Exception as e:
        logger.error(f"Timezone calculation error: {e}")
        return "UTC"

def get_local_time(tz_str: str) -> str:
    """Calculate local time for given timezone"""
    try:
        tz = pytz.timezone(tz_str)
        local_time = datetime.now(tz)
        return local_time.strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception as e:
        logger.error(f"Time calculation error: {e}")
        return "Unknown"

async def get_nasa_image() -> tuple[str, str, str] | None:
    """Retrieve NASA Astronomy Picture of the Day with caching"""
    global nasa_cache
    now = datetime.now()
    
    # Check cache
    if nasa_cache is not None:
        cached_data, timestamp = nasa_cache
        if now - timestamp < NASA_CACHE_TTL:
            logger.debug("NASA cache hit")
            return cached_data
    
    # Fetch new data
    try:
        url = f"https://api.nasa.gov/planetary/apod?api_key={NASA_API_KEY}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                response.raise_for_status()
                data = await response.json()
        
        result = (data["url"], data.get("title", "NASA Sky Image"), data.get("explanation", ""))
        nasa_cache = (result, now)
        logger.info("Fetched NASA APOD")
        return result
        
    except Exception as e:
        logger.error(f"NASA API error: {e}")
        return None

def cardinal_direction(degrees: float) -> str:
    """Convert wind direction from degrees to cardinal direction"""
    directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                  "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    idx = round(degrees / 22.5) % 16
    return directions[idx]

# ========================
# WEB SERVER (Keep-Alive for some hosting services)
# ========================
app = Flask("")

@app.route("/")
def home():
    return "Atmospheric Monitoring System Online"

@app.route("/health")
def health():
    return {"status": "healthy", "users": len(user_settings)}

def run_web_server():
    app.run(host="0.0.0.0", port=8080)

Thread(target=run_web_server, daemon=True).start()

# ========================
# SLASH COMMANDS
# ========================

@tree.command(name="ping", description="System diagnostics check")
@app_commands.checks.cooldown(1, 10)
async def ping(interaction: discord.Interaction):
    try:
        latency = round(bot.latency * 1000, 2)
        await interaction.response.send_message(
            f"🛰️ **System Status**: Online\n"
            f"**Latency**: {latency}ms\n"
            f"**Active Stations**: {len(user_settings)}"
        )
    except Exception as e:
        logger.error(f"Ping command error: {e}")
        await interaction.response.send_message("❌ Error checking system status.")

@tree.command(name="setlocation", description="Configure observation station coordinates")
async def setlocation(interaction: discord.Interaction, city: str):
    try:
        # Validate input
        city = city.strip()
        if len(city) == 0 or len(city) > 100:
            await interaction.response.send_message("❌ Invalid city name. Please provide a valid city (1-100 characters).")
            return
        
        await interaction.response.defer()
        
        result = await get_weather(city)
        if not result:
            await interaction.followup.send(f"❌ **Error**: Unable to resolve location: `{city}`\nVerify city name and retry.")
            return
        
        lat, lon = result["lat"], result["lon"]
        tz_str = get_timezone_from_coords(lat, lon)
        local_time = get_local_time(tz_str)
        
        user_settings[interaction.user.id] = {
            "city": result["city_name"],
            "country": result["country"],
            "lat": lat,
            "lon": lon,
            "tz": tz_str,
            "temp_unit": "celsius",
            "report_hour": 8
        }
        
        save_user_settings()
        
        embed = discord.Embed(
            title="📍 Observation Station Configured",
            description=f"Location: **{result['city_name']}, {result['country']}**",
            color=0x2C3E50
        )
        embed.add_field(name="Coordinates", value=f"{lat:.4f}°, {lon:.4f}°", inline=True)
        embed.add_field(name="Timezone", value=f"`{tz_str}`", inline=True)
        embed.add_field(name="Local Time", value=local_time, inline=False)
        embed.add_field(name="Temperature Unit", value="Celsius (use `/settings` to change)", inline=True)
        embed.set_footer(text="Daily atmospheric reports scheduled for 08:00 local time")
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        logger.error(f"Setlocation command error: {e}")
        await interaction.followup.send("❌ An error occurred while setting location.")

@tree.command(name="atmosphere", description="Generate comprehensive atmospheric analysis")
@app_commands.checks.cooldown(1, COMMAND_COOLDOWN_SECONDS)
async def atmosphere(interaction: discord.Interaction):
    try:
        if interaction.user.id not in user_settings:
            await interaction.response.send_message("⚠️ No observation station configured. Execute `/setlocation` first.")
            return
        
        await interaction.response.defer()
        
        settings = user_settings[interaction.user.id]
        city = settings["city"]
        temp_unit = settings.get("temp_unit", "celsius")
        
        data = await get_weather(city)
        if not data:
            await interaction.followup.send("❌ **Data Acquisition Failed**: Unable to retrieve atmospheric data.")
            return
        
        # Calculations
        dewpoint = calculate_dewpoint(data["temp"], data["humidity"])
        heat_index = calculate_heat_index(data["temp"], data["humidity"])
        air_density = calculate_air_density(data["temp"], data["pressure"], data["humidity"])
        cloud_class, cloud_desc, oktas = classify_clouds_scientific(data["clouds"])
        cloud_base = calculate_cloud_base_height(data["temp"], dewpoint)
        wind_dir = cardinal_direction(data["wind_deg"])
        visibility = get_visibility_category(data["visibility"])
        local_time = get_local_time(settings["tz"])
        
        # Temperature conversions
        temp, unit_symbol = convert_temperature(data["temp"], temp_unit)
        feels, _ = convert_temperature(data["feels_like"], temp_unit)
        hi, _ = convert_temperature(heat_index, temp_unit)
        dew, _ = convert_temperature(dewpoint, temp_unit)
        
        # Main embed
        embed = discord.Embed(
            title=f"🌐 Atmospheric Analysis: {city}, {settings['country']}",
            description=f"**Observation Time**: {local_time}",
            color=0x34495E
        )
        
        temp_data = (
            f"**Temperature**: {temp}{unit_symbol}\n"
            f"**Apparent Temperature**: {feels}{unit_symbol}\n"
            f"**Heat Index**: {hi}{unit_symbol}\n"
            f"**Dewpoint**: {dew}{unit_symbol}"
        )
        embed.add_field(name="🌡️ Thermal Conditions", value=temp_data, inline=False)
        
        pressure_data = (
            f"**Pressure**: {data['pressure']} hPa\n"
            f"**Relative Humidity**: {data['humidity']}%\n"
            f"**Air Density**: {air_density} kg/m³"
        )
        embed.add_field(name="🔬 Atmospheric Composition", value=pressure_data, inline=False)
        
        cloud_data = (
            f"**Coverage**: {data['clouds']}% ({oktas}/8 oktas)\n"
            f"**Classification**: {cloud_class}\n"
            f"**Type**: {cloud_desc}\n"
            f"**Estimated Base**: {cloud_base}m AGL"
        )
        embed.add_field(name="☁️ Nephoanalysis", value=cloud_data, inline=False)
        
        wind_data = (
            f"**Wind Speed**: {data['wind_speed']} m/s\n"
            f"**Direction**: {data['wind_deg']}° ({wind_dir})\n"
            f"**Visibility**: {visibility}"
        )
        embed.add_field(name="💨 Wind & Visibility", value=wind_data, inline=False)
        
        embed.add_field(
            name="📊 Current Conditions",
            value=f"{data['description'].capitalize()}",
            inline=False
        )
        
        embed.set_footer(text=f"Data source: OpenWeather API | Station: {settings['lat']:.4f}°, {settings['lon']:.4f}°")
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        logger.error(f"Atmosphere command error: {e}")
        await interaction.followup.send("❌ An error occurred generating the atmospheric report.")

@tree.command(name="nasa", description="Retrieve NASA Astronomy Picture of the Day")
@app_commands.checks.cooldown(1, 60)
async def nasa(interaction: discord.Interaction):
    try:
        await interaction.response.defer()
        
        image = await get_nasa_image()
        if not image:
            await interaction.followup.send("❌ **NASA API Error**: Unable to retrieve astronomical image.")
            return
        
        url, title, explanation = image
        desc_short = explanation[:500]
        if len(explanation) > 500:
            desc_short += "..."
        
        embed = discord.Embed(
            title=f"🔭 NASA APOD: {title}",
            description=desc_short,
            color=0x1C1C3C
        )
        embed.set_image(url=url)
        embed.set_footer(text="Source: NASA Astronomy Picture of the Day")
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        logger.error(f"NASA command error: {e}")
        await interaction.followup.send("❌ An error occurred retrieving the NASA image.")

@tree.command(name="station", description="Display observation station configuration")
async def station(interaction: discord.Interaction):
    try:
        if interaction.user.id not in user_settings:
            await interaction.response.send_message("⚠️ No station configured. Initialize with `/setlocation`.")
            return
        
        settings = user_settings[interaction.user.id]
        local_time = get_local_time(settings["tz"])
        temp_unit = settings.get("temp_unit", "celsius").capitalize()
        report_hour = settings.get("report_hour", 8)
        
        embed = discord.Embed(
            title="🛰️ Observation Station Status",
            color=0x27AE60
        )
        embed.add_field(name="Location", value=f"{settings['city']}, {settings['country']}", inline=False)
        embed.add_field(name="Coordinates", value=f"{settings['lat']:.4f}°, {settings['lon']:.4f}°", inline=True)
        embed.add_field(name="Timezone", value=f"`{settings['tz']}`", inline=True)
        embed.add_field(name="Local Time", value=local_time, inline=False)
        embed.add_field(name="Temperature Unit", value=temp_unit, inline=True)
        embed.add_field(name="Report Schedule", value=f"Daily at {report_hour:02d}:00 local time", inline=True)
        embed.set_footer(text="Automated atmospheric monitoring active")
        
        await interaction.response.send_message(embed=embed)
        
    except Exception as e:
        logger.error(f"Station command error: {e}")
        await interaction.response.send_message("❌ Error retrieving station information.")

@tree.command(name="settings", description="Configure your preferences")
@app_commands.describe(
    temperature_unit="Temperature display unit",
    report_hour="Hour for daily report (0-23)"
)
@app_commands.choices(temperature_unit=[
    app_commands.Choice(name="Celsius", value="celsius"),
    app_commands.Choice(name="Fahrenheit", value="fahrenheit"),
    app_commands.Choice(name="Kelvin", value="kelvin")
])
async def settings(
    interaction: discord.Interaction,
    temperature_unit: app_commands.Choice[str] = None,
    report_hour: int = None
):
    try:
        if interaction.user.id not in user_settings:
            await interaction.response.send_message("⚠️ Configure a location first with `/setlocation`.")
            return
        
        settings = user_settings[interaction.user.id]
        changes = []
        
        if temperature_unit is not None:
            settings["temp_unit"] = temperature_unit.value
            changes.append(f"Temperature unit: **{temperature_unit.name}**")
        
        if report_hour is not None:
            if not 0 <= report_hour <= 23:
                await interaction.response.send_message("❌ Report hour must be between 0 and 23.")
                return
            settings["report_hour"] = report_hour
            changes.append(f"Daily report time: **{report_hour:02d}:00**")
        
        if not changes:
            # Show current settings
            temp_unit = settings.get("temp_unit", "celsius").capitalize()
            rep_hour = settings.get("report_hour", 8)
            
            embed = discord.Embed(
                title="⚙️ Current Settings",
                color=0x3498DB
            )
            embed.add_field(name="Temperature Unit", value=temp_unit, inline=True)
            embed.add_field(name="Daily Report Time", value=f"{rep_hour:02d}:00", inline=True)
            embed.set_footer(text="Use command parameters to change settings")
            
            await interaction.response.send_message(embed=embed)
        else:
            save_user_settings()
            
            embed = discord.Embed(
                title="✅ Settings Updated",
                description="\n".join(changes),
                color=0x27AE60
            )
            await interaction.response.send_message(embed=embed)
            
    except Exception as e:
        logger.error(f"Settings command error: {e}")
        await interaction.response.send_message("❌ Error updating settings.")

@tree.command(name="help", description="Display all available commands")
async def help_command(interaction: discord.Interaction):
    try:
        embed = discord.Embed(
            title="🛰️ Cirrus Uncinus - Command Reference",
            description="Atmospheric Monitoring System",
            color=0x3498DB
        )
        
        commands_info = [
            ("📍 /setlocation", "Configure your observation station location"),
            ("🌐 /atmosphere", "Get comprehensive atmospheric analysis"),
            ("🛰️ /station", "View your station configuration"),
            ("⚙️ /settings", "Configure temperature units and report schedule"),
            ("🔭 /nasa", "NASA Astronomy Picture of the Day"),
            ("🏓 /ping", "Check bot status and latency"),
            ("❓ /help", "Display this help message")
        ]
        
        for cmd, desc in commands_info:
            embed.add_field(name=cmd, value=desc, inline=False)
        
        embed.set_footer(text="Daily reports are sent automatically at your configured time")
        
        await interaction.response.send_message(embed=embed)
        
    except Exception as e:
        logger.error(f"Help command error: {e}")
        await interaction.response.send_message("❌ Error displaying help information.")

# ========================
# SCHEDULED DAILY REPORTS
# ========================

async def send_daily_report(user_id: int, settings: dict):
    """Send daily atmospheric report to a user"""
    try:
        user = await bot.fetch_user(user_id)
        city = settings["city"]
        country = settings["country"]
        temp_unit = settings.get("temp_unit", "celsius")

        data = await get_weather(city)
        if not data:
            logger.warning(f"Could not fetch weather for daily report: {city}")
            return
        
        # Calculations
        dewpoint = calculate_dewpoint(data["temp"], data["humidity"])
        heat_index = calculate_heat_index(data["temp"], data["humidity"])
        air_density = calculate_air_density(data["temp"], data["pressure"], data["humidity"])
        cloud_class, cloud_desc, oktas = classify_clouds_scientific(data["clouds"])
        cloud_base = calculate_cloud_base_height(data["temp"], dewpoint)
        wind_dir = cardinal_direction(data["wind_deg"])
        local_time = get_local_time(settings["tz"])
        
        # Temperature conversions
        temp, unit_symbol = convert_temperature(data["temp"], temp_unit)
        feels, _ = convert_temperature(data["feels_like"], temp_unit)
        hi, _ = convert_temperature(heat_index, temp_unit)
        dew, _ = convert_temperature(dewpoint, temp_unit)
        
        embed = discord.Embed(
            title=f"📡 Daily Atmospheric Report: {city}, {country}",
            description=f"**{local_time}**\n\n*Automated observational data summary*",
            color=0x2980B9
        )
        
        summary = (
            f"**Temperature**: {temp}{unit_symbol} (feels like {feels}{unit_symbol})\n"
            f"**Dewpoint**: {dew}{unit_symbol} | **Heat Index**: {hi}{unit_symbol}\n"
            f"**Pressure**: {data['pressure']} hPa | **Humidity**: {data['humidity']}%\n"
            f"**Air Density**: {air_density} kg/m³"
        )
        embed.add_field(name="🌡️ Thermodynamic Data", value=summary, inline=False)
        
        cloud_summary = (
            f"**Coverage**: {data['clouds']}% ({oktas}/8 oktas) - {cloud_class}\n"
            f"**Type**: {cloud_desc}\n"
            f"**Base Height**: ~{cloud_base}m AGL"
        )
        embed.add_field(name="☁️ Cloud Analysis", value=cloud_summary, inline=False)
        
        wind_summary = f"**{data['wind_speed']} m/s** from **{wind_dir}** ({data['wind_deg']}°)"
        embed.add_field(name="💨 Wind Conditions", value=wind_summary, inline=False)
        
        embed.add_field(name="📊 Observations", value=data['description'].capitalize(), inline=False)
        
        report_hour = settings.get("report_hour", 8)
        embed.set_footer(text=f"Station: {settings['lat']:.4f}°, {settings['lon']:.4f}° | Next report: {report_hour:02d}:00 tomorrow")
        
        await user.send(embed=embed)
        logger.info(f"Daily report sent to user {user_id}")

        # NASA Image
        image = await get_nasa_image()
        if image:
            url, title, explanation = image
            desc_short = explanation[:500]
            if len(explanation) > 500:
                desc_short += "..."
                
            nasa_embed = discord.Embed(
                title=f"🔭 NASA APOD: {title}",
                description=desc_short,
                color=0x1C1C3C
            )
            nasa_embed.set_image(url=url)
            await user.send(embed=nasa_embed)
            
    except discord.Forbidden:
        logger.warning(f"Cannot send DM to user {user_id} - DMs disabled")
    except Exception as e:
        logger.error(f"Report transmission error for user {user_id}: {e}")

async def check_and_send_reports():
    """Check all users and send reports if it's their scheduled time"""
    logger.info("Checking for scheduled reports...")
    for user_id, settings in user_settings.items():
        try:
            tz_str = settings["tz"]
            report_hour = settings.get("report_hour", 8)
            
            tz = pytz.timezone(tz_str)
            local_time = datetime.now(tz)
            
            # Send if current hour matches report hour (with 5-minute buffer)
            if local_time.hour == report_hour and local_time.minute < 5:
                await send_daily_report(user_id, settings)
                
        except Exception as e:
            logger.error(f"Error checking report schedule for user {user_id}: {e}")

# ========================
# ERROR HANDLERS
# ========================

@tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CommandOnCooldown):
        await interaction.response.send_message(
            f"⏳ Command on cooldown. Try again in {error.retry_after:.1f} seconds.",
            ephemeral=True
        )
    else:
        logger.error(f"Command error: {error}")
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message("❌ An error occurred processing your command.", ephemeral=True)
            else:
                await interaction.followup.send("❌ An error occurred processing your command.", ephemeral=True)
        except:
            pass

# ========================
# EVENTS
# ========================

@bot.event
async def on_ready():
    load_user_settings()
    await tree.sync()
    logger.info(f"🛰️ Atmospheric Monitoring System initialized: {bot.user}")
    logger.info(f"📡 Stations configured: {len(user_settings)}")
    
    # Start scheduler
    scheduler = AsyncIOScheduler()
    
    # Run every 5 minutes to check for reports
    scheduler.add_job(check_and_send_reports, 'cron', minute='*/5')
    
    scheduler.start()
    logger.info("⏰ Report scheduler started")

@bot.event
async def on_disconnect():
    logger.warning("Bot disconnected from Discord")

@bot.event
async def on_resumed():
    logger.info("Bot connection resumed")

# ========================
# GRACEFUL SHUTDOWN
# ========================

import signal
import sys

def signal_handler(sig, frame):
    logger.info("Shutting down gracefully...")
    save_user_settings()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ========================
# INITIALIZE
# ========================

if __name__ == "__main__":
    try:
        bot.run(BOT_TOKEN)
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        save_user_settings()