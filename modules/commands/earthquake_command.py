#!/usr/bin/env python3
"""
Earthquake command for the MeshCore Bot
Provides recent earthquake information using USGS Earthquake API
"""

import requests
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from .base_command import BaseCommand
from ..models import MeshMessage
from ..utils import calculate_distance


class EarthquakeCommand(BaseCommand):
    """Handles earthquake commands using USGS Earthquake API.

    Provides recent earthquake information for specified locations or globally.
    Uses the USGS Earthquake Hazards Program API which is free and requires no API key.
    """

    # Plugin metadata
    name = "earthquake"
    keywords = ['earthquake', 'eq', 'quake', 'seismic']
    description = "Get recent earthquake info (usage: eq, eq 5, eq tokyo, eq 47.6,-122.3)"
    category = "weather"
    cooldown_seconds = 5  # 5 second cooldown per user to prevent API abuse
    requires_internet = True  # Requires internet access for USGS API

    # USGS API endpoint
    USGS_API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

    # Default search parameters
    DEFAULT_MIN_MAGNITUDE = 2.5  # Minimum magnitude to report
    DEFAULT_DAYS = 1  # Default to last 24 hours
    DEFAULT_LIMIT = 5  # Default number of earthquakes to show
    MAX_LIMIT = 10  # Maximum earthquakes to show

    def __init__(self, bot):
        super().__init__(bot)
        self.enabled = self.get_config_value('Earthquake_Command', 'enabled', fallback=True, value_type='bool')
        self.url_timeout = 10  # seconds

        # Get default location from config (for nearby search)
        self.default_lat = self.bot.config.getfloat('Weather', 'my_position_lat', fallback=None)
        self.default_lon = self.bot.config.getfloat('Weather', 'my_position_lon', fallback=None)

        # If not in Weather section, try Weather_Service section
        if self.default_lat is None:
            self.default_lat = self.bot.config.getfloat('Weather_Service', 'my_position_lat', fallback=None)
        if self.default_lon is None:
            self.default_lon = self.bot.config.getfloat('Weather_Service', 'my_position_lon', fallback=None)

        # Search radius in km for location-based queries
        self.search_radius_km = self.get_config_value('Earthquake_Command', 'search_radius_km', fallback=500, value_type='int')

        # Minimum magnitude from config
        self.min_magnitude = self.get_config_value('Earthquake_Command', 'min_magnitude', fallback=2.5, value_type='float')

    def get_help_text(self) -> str:
        return "Usage: eq [magnitude|location|count] - Get recent earthquakes. Examples: eq, eq 5, eq 6.0, eq tokyo, eq 47.6,-122.3, eq 10 (show 10)"

    def can_execute(self, message: MeshMessage) -> bool:
        """Check if this command can be executed."""
        if not self.enabled:
            return False
        return super().can_execute(message)

    async def execute(self, message: MeshMessage) -> bool:
        """Execute the earthquake command.

        Args:
            message: The input message trigger.

        Returns:
            bool: True if execution was successful.
        """
        content = message.content.strip()
        parts = content.split()

        # Default parameters
        min_mag = self.min_magnitude
        max_results = DEFAULT_LIMIT = 5
        location = None
        lat = None
        lon = None

        # Parse arguments
        if len(parts) > 1:
            arg = ' '.join(parts[1:]).strip()

            # Check if it's a number (magnitude threshold or count)
            try:
                num = float(arg)
                if num >= 1 and num <= 10:
                    # Likely a magnitude (1.0-10.0)
                    min_mag = num
                elif num > 10:
                    # Likely a count
                    max_results = min(int(num), self.MAX_LIMIT)
                else:
                    min_mag = num
            except ValueError:
                # Check if it's coordinates (lat,lon)
                if ',' in arg and self._is_coordinates(arg):
                    lat, lon = self._parse_coordinates(arg)
                else:
                    # It's a location name
                    location = arg

        # If location name provided, geocode it
        if location:
            coords = await self._geocode_location(location)
            if coords:
                lat, lon = coords
            else:
                await self.send_response(message, f"Could not find location: {location}")
                return True

        # Use bot's default location if no location specified
        if lat is None or lon is None:
            if self.default_lat is not None and self.default_lon is not None:
                lat, lon = self.default_lat, self.default_lon
            else:
                await self.send_response(message, "No location specified and bot location not configured")
                return True

        # Fetch earthquakes
        try:
            earthquakes = await self._fetch_earthquakes(lat, lon, min_mag, max_results)
            # Sort by distance (nearest first)
            earthquakes = self._sort_by_distance(earthquakes, lat, lon)
            location_desc = f"within {self.search_radius_km}km"
            ref_coords = (lat, lon)

            if not earthquakes:
                await self.send_response(message, f"No M{min_mag}+ earthquakes {location_desc} in last 24h")
                return True

            # Format response
            response = self._format_earthquakes(earthquakes, min_mag, location_desc, ref_coords)
            await self.send_response(message, response)
            return True

        except requests.exceptions.Timeout:
            await self.send_response(message, "Earthquake data request timed out")
            return True
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching earthquake data: {e}")
            await self.send_response(message, "Error fetching earthquake data")
            return True
        except Exception as e:
            self.logger.error(f"Earthquake command error: {e}")
            await self.send_response(message, "Error processing earthquake request")
            return True

    def _sort_by_distance(self, earthquakes: List[Dict[str, Any]], ref_lat: float, ref_lon: float) -> List[Dict[str, Any]]:
        """Sort earthquakes by distance from reference point (nearest first)."""
        def get_distance(eq):
            geom = eq.get('geometry', {})
            coords = geom.get('coordinates', [0, 0, 0])
            if len(coords) >= 2:
                eq_lon, eq_lat = coords[0], coords[1]  # GeoJSON is [lon, lat, depth]
                return calculate_distance(ref_lat, ref_lon, eq_lat, eq_lon)
            return float('inf')

        return sorted(earthquakes, key=get_distance)

    def _is_coordinates(self, text: str) -> bool:
        """Check if text looks like coordinates."""
        import re
        return bool(re.match(r'^\s*-?\d+\.?\d*\s*,\s*-?\d+\.?\d*\s*$', text))

    def _parse_coordinates(self, text: str) -> tuple:
        """Parse lat,lon from text."""
        parts = text.split(',')
        lat = float(parts[0].strip())
        lon = float(parts[1].strip())
        return (lat, lon)

    async def _geocode_location(self, location: str) -> Optional[tuple]:
        """Geocode a location name to coordinates."""
        try:
            from ..utils import rate_limited_nominatim_geocode_sync
            result = rate_limited_nominatim_geocode_sync(self.bot, location, timeout=5)
            if result:
                return (result.latitude, result.longitude)
        except Exception as e:
            self.logger.debug(f"Geocoding error for {location}: {e}")
        return None

    def _shorten_url(self, url: str) -> str:
        """Shorten a URL using is.gd service (free, no API key required).

        Args:
            url: The URL to shorten

        Returns:
            Shortened URL, or original URL if shortening fails
        """
        if not url:
            return url

        try:
            response = requests.get(
                'https://is.gd/create.php',
                params={'format': 'simple', 'url': url},
                timeout=3
            )
            if response.status_code == 200 and response.text.startswith('https://'):
                return response.text.strip()
        except Exception as e:
            self.logger.debug(f"URL shortening failed: {e}")

        return url  # Return original URL if shortening fails

    async def _fetch_earthquakes(self, lat: float, lon: float, min_mag: float, limit: int) -> List[Dict[str, Any]]:
        """Fetch recent earthquakes near a location."""
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=self.DEFAULT_DAYS)

        params = {
            'format': 'geojson',
            'starttime': start_time.strftime('%Y-%m-%dT%H:%M:%S'),
            'endtime': end_time.strftime('%Y-%m-%dT%H:%M:%S'),
            'minmagnitude': min_mag,
            'latitude': lat,
            'longitude': lon,
            'maxradiuskm': self.search_radius_km,
            'orderby': 'time',
            'limit': limit
        }

        response = requests.get(self.USGS_API_URL, params=params, timeout=self.url_timeout)
        response.raise_for_status()
        data = response.json()

        return data.get('features', [])

    def _format_earthquakes(self, earthquakes: List[Dict[str, Any]], min_mag: float, location_desc: str, ref_coords: Optional[tuple] = None) -> str:
        """Format earthquake data for display.

        Args:
            earthquakes: List of earthquake data from USGS API
            min_mag: Minimum magnitude filter used
            location_desc: Description of search location
            ref_coords: Optional (lat, lon) tuple for distance calculation
        """
        if not earthquakes:
            return f"No M{min_mag}+ earthquakes {location_desc}"

        lines = []

        # Header with count
        count = len(earthquakes)
        header = f"Recent quakes (M{min_mag}+):"
        lines.append(header)

        for eq in earthquakes[:5]:  # Limit to 5 for message size
            props = eq.get('properties', {})
            geom = eq.get('geometry', {})
            coords = geom.get('coordinates', [0, 0, 0])

            mag = props.get('mag', 0)
            place = props.get('place', 'Unknown')
            time_ms = props.get('time', 0)
            depth = coords[2] if len(coords) > 2 else 0
            url = props.get('url', '')

            # Get magnitude emoji
            emoji = self._get_magnitude_emoji(mag)

            # Calculate distance if reference coordinates provided
            distance_str = ""
            if ref_coords and len(coords) >= 2:
                eq_lon, eq_lat = coords[0], coords[1]  # GeoJSON is [lon, lat, depth]
                distance = calculate_distance(ref_coords[0], ref_coords[1], eq_lat, eq_lon)
                distance_str = f"{distance:.0f}km "

            # Format time as relative
            time_ago = self._format_time_ago(time_ms)

            # Shorten place name if needed
            place_short = self._shorten_place(place)

            # Format: 🟡M5.2 123km 45km NW of Tokyo (2h, 10km)
            line = f"{emoji}M{mag:.1f} {distance_str}{place_short} ({time_ago}, {depth:.0f}km)"
            lines.append(line)

            # Add shortened URL on separate line if available
            if url:
                short_url = self._shorten_url(url)
                lines.append(short_url)

        if count > 5:
            lines.append(f"+{count - 5} more")

        return '\n'.join(lines)

    def _format_time_ago(self, time_ms: int) -> str:
        """Format timestamp as relative time."""
        if not time_ms:
            return "?"

        try:
            eq_time = datetime.fromtimestamp(time_ms / 1000, tz=timezone.utc)
            now = datetime.now(timezone.utc)
            diff = now - eq_time

            total_seconds = int(diff.total_seconds())

            if total_seconds < 60:
                return f"{total_seconds}s"
            elif total_seconds < 3600:
                minutes = total_seconds // 60
                return f"{minutes}m"
            elif total_seconds < 86400:
                hours = total_seconds // 3600
                return f"{hours}h"
            else:
                days = total_seconds // 86400
                return f"{days}d"
        except Exception:
            return "?"

    def _shorten_place(self, place: str, max_len: int = 35) -> str:
        """Shorten place name for compact display."""
        if not place:
            return "Unknown"

        # Remove common prefixes
        prefixes_to_remove = ['km ', 'mi ']
        for prefix in prefixes_to_remove:
            if prefix in place.lower():
                # Keep the direction and location
                pass

        # Truncate if too long
        if len(place) > max_len:
            place = place[:max_len-2] + ".."

        return place

    def _get_magnitude_emoji(self, mag: float) -> str:
        """Get emoji based on magnitude."""
        if mag >= 7.0:
            return "🔴"  # Major
        elif mag >= 6.0:
            return "🟠"  # Strong
        elif mag >= 5.0:
            return "🟡"  # Moderate
        elif mag >= 4.0:
            return "🟢"  # Light
        else:
            return "⚪"  # Minor
