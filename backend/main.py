# main.py - PROFESSIONAL VERSION WITH LOGGING
import logging
import os
import sys
import psutil
from datetime import datetime
from dq_unified import main as unified_main
from dq_comparison import main as comparison_main
from dq_advanced import main as advanced_main
from dq_rules import main as rules_main
from app_config import APP_SETTINGS, FILE_PATHS

def check_system_resources():
    """Check system resources before starting analysis"""
    logger = logging.getLogger(__name__)
    logger.info("Starting system resource check")
    
    try:
        # Check memory
        memory = psutil.virtual_memory()
        total_memory_gb = memory.total / (1024**3)
        available_memory_gb = memory.available / (1024**3)
        memory_percent_used = memory.percent
        
        logger.info(f"Total RAM: {total_memory_gb:.1f} GB")
        logger.info(f"Available RAM: {available_memory_gb:.1f} GB")
        logger.info(f"Memory used: {memory_percent_used:.1f}%")
        
        memory_status = "OK"
        if available_memory_gb < 0.5:
            memory_status = "CRITICAL"
            logger.warning("Very low memory available (<0.5GB). Consider closing other applications")
        elif available_memory_gb < 1:
            memory_status = "WARNING"
            logger.warning("Low memory available (<1GB). For large datasets, consider using sampling")
        else:
            logger.info("Memory status: OK")
        
        # Check disk space
        disk = psutil.disk_usage('.')
        free_disk_gb = disk.free / (1024**3)
        disk_percent_used = disk.percent
        
        logger.info(f"Free disk space: {free_disk_gb:.1f} GB")
        logger.info(f"Disk used: {disk_percent_used:.1f}%")
        
        if free_disk_gb < 1:
            logger.warning("Low disk space (<1GB). Temporary files may not be created properly")
        else:
            logger.info("Disk space: OK")
        
        # Check CPU
        cpu_percent = psutil.cpu_percent(interval=0.5)
        cpu_count = psutil.cpu_count()
        
        logger.info(f"CPU cores: {cpu_count}")
        logger.info(f"CPU usage: {cpu_percent:.1f}%")
        
        if cpu_percent > 90:
            logger.warning("High CPU usage")
        
        # Python info
        logger.info(f"Python version: {sys.version.split()[0]}")
        logger.info(f"System platform: {sys.platform}")
        
        return memory_status
        
    except Exception as e:
        logger.error(f"Could not check system resources: {e}")
        return "UNKNOWN"

def setup_logging(ui_mode=False):
    """Setup comprehensive logging for the framework"""
    log_dir = FILE_PATHS['log_directory']
    
    # Always ensure log directory exists
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Get the root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers to avoid duplicates
    if logger.handlers:
        logger.handlers.clear()
    
    if ui_mode:
        # UI/API MODE: Minimal console output, detailed file logging
        log_filename = f"{log_dir}/dq_api_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        # File handler for API logs
        file_handler = logging.FileHandler(log_filename, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_format)
        
        # Console handler (minimal output for API)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.WARNING)
        console_format = logging.Formatter('%(levelname)s - %(message)s')
        console_handler.setFormatter(console_format)
        
        # Add both handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        logger.info("API Mode Logging Initialized")
        logger.info(f"API logs will be saved to: {log_filename}")
        
    else:
        # CLI MODE: File + Console logging
        log_filename = f"{log_dir}/dq_framework_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        # File handler
        file_handler = logging.FileHandler(log_filename, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_format)
        
        # Console handler for CLI
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter('%(levelname)s - %(message)s')
        console_handler.setFormatter(console_format)
        
        # Add both handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        logger.info("CLI Mode Logging Initialized")
        logger.info(f"CLI logs will be saved to: {log_filename}")
    
    return logging.getLogger(__name__)

# Initialize logger
logger = setup_logging()

def show_welcome_message():
    """Display welcome message and framework information"""
    print("\n" + "="*70)
    print("🚀 UNIVERSAL DATA QUALITY FRAMEWORK")
    print("="*70)
    print("⭐ Supports: CSV, Excel, Databases (PostgreSQL, MySQL, SQLite, Oracle, SQL Server)")
    print("⭐ Optimized for large datasets (up to 1M+ rows)")
    print("⭐ Smart memory management and progress tracking")
    print("⭐ Hierarchical database browsing (Database→Schema→Table)")
    print(f"⭐ Config File Mode: {'ENABLED' if APP_SETTINGS.get('use_config_file', False) else 'DISABLED'}")
    print("⭐ Set environment variable DQ_USE_CONFIG_FILE=true to use config file")
    print("="*70)

def show_large_dataset_tips():
    """Display tips for working with large datasets"""
    print("\n💡 TIPS FOR LARGE DATASETS:")
    print("   • For datasets > 500k rows, consider using sampling")
    print("   • Close other memory-intensive applications")
    print("   • For very large comparisons, run during off-peak hours")
    print("   • Use database sources when possible (more efficient)")
    print("   • Monitor memory usage in the application")

def main():
    """Main function with user selection"""
    logger.info("🚀 UNIVERSAL DATA QUALITY FRAMEWORK INITIALIZED")
    
    show_welcome_message()
    
    # Check system resources
    memory_status = check_system_resources()
    
    # Show large dataset tips if memory is low
    if memory_status in ["WARNING", "CRITICAL"]:
        show_large_dataset_tips()
        logger.warning(f"Based on system check, your memory status is: {memory_status}")
        proceed = input("   Continue anyway? (y/n): ").strip().lower()
        if proceed != 'y':
            logger.info("Exiting framework...")
            return
    
    session_start = datetime.now()
    logger.info(f"User session started at: {session_start}")
    
    while True:
        logger.info("Displaying main menu options to user")
        print("\n" + "="*60)
        print("📋 MAIN MENU - SELECT ANALYSIS MODE:")
        print("="*60)
        print("1. 🔍 Single Source Analysis")
        print("   - CSV, Excel, or Database quality checks")
        print("   - Find nulls, duplicates, format issues")
        print("   - Show exact problem rows")
        print("   - ✅ Optimized for large datasets")
        
        print("\n2. 🔄 Source-Target Comparison") 
        print("   - Compare any two data sources")
        print("   - CSV vs CSV, DB vs DB, CSV vs DB")
        print("   - Find differences between systems")
        print("   - ✅ Smart normalization for accurate comparison")
        
        print("\n3. ⚡ Advanced Data Checks")
        print("   - Whitespace, zero-padding validation")
        print("   - Data formatting rules")
        print("   - Advanced cleaning checks")
        print("   - ✅ Memory-efficient processing")
        print("   - ⭐ Hierarchical database selection (browse databases, schemas, tables)")
        
        print("\n4. 📈 Business Rules Engine")
        print("   - Custom KPI validation")
        print("   - Threshold monitoring")
        print("   - Growth rate checks")
        print("   - ✅ Text comparison capabilities")
        
        print("\n5. 📊 System & Performance")
        print("   - Check system resources")
        print("   - View audit logs")
        print("   - Performance settings")
        
        print("\n6. ❌ Exit")
        print("="*60)
        
        choice = input("\nEnter your choice (1-6): ").strip()
        logger.info(f"User selected option: {choice}")
        
        try:
            if choice == "1":
                logger.info("Starting Single Source Analysis module")
                print("\n" + "="*50)
                print("🔍 STARTING SINGLE SOURCE ANALYSIS")
                print("="*50)
                print("⚠️  Note: For datasets > 200k rows, loading may take time")
                unified_main()
                logger.info("Single Source Analysis completed successfully")
                
            elif choice == "2":
                logger.info("Starting Source-Target Comparison module")
                print("\n" + "="*50)
                print("🔄 STARTING SOURCE-TARGET COMPARISON")
                print("="*50)
                print("⚠️  Note: Comparing large datasets may take time")
                comparison_main()
                logger.info("Source-Target Comparison completed successfully")
                
            elif choice == "3":
                logger.info("Starting Advanced Data Checks module")
                print("\n" + "="*50)
                print("⚡ STARTING ADVANCED DATA CHECKS")
                print("="*50)
                advanced_main()
                logger.info("Advanced Data Checks completed successfully")
                
            elif choice == "4":
                logger.info("Starting Business Rules Engine module")
                print("\n" + "="*50)
                print("📈 STARTING BUSINESS RULES ENGINE")
                print("="*50)
                rules_main()
                logger.info("Business Rules Engine completed successfully")
                
            elif choice == "5":
                logger.info("Showing system and performance options")
                print("\n" + "="*50)
                print("📊 SYSTEM & PERFORMANCE")
                print("="*50)
                
                print("\nSelect option:")
                print("1. Check current system resources")
                print("2. View memory optimization tips")
                print("3. Check framework settings")
                print("4. Back to main menu")
                
                sys_choice = input("\nEnter choice (1-4): ").strip()
                
                if sys_choice == "1":
                    check_system_resources()
                elif sys_choice == "2":
                    show_large_dataset_tips()
                elif sys_choice == "3":
                    print(f"\n📋 FRAMEWORK SETTINGS:")
                    print(f"   • Max rows in memory: {APP_SETTINGS.get('max_rows_in_memory', 200000):,}")
                    print(f"   • Large dataset threshold: {APP_SETTINGS.get('large_dataset_threshold', 50000):,}")
                    print(f"   • Streaming batch size: {APP_SETTINGS.get('streaming_batch_size', 10000):,}")
                    print(f"   • Audit logging: {'Enabled' if APP_SETTINGS.get('audit_enabled', True) else 'Disabled'}")
                    print(f"   • Fallback logging: {'Enabled' if APP_SETTINGS.get('fallback_logging', True) else 'Disabled'}")
                else:
                    print("Returning to main menu...")
                
            elif choice == "6":
                session_end = datetime.now()
                session_duration = session_end - session_start
                
                logger.info(f"User session ended. Duration: {session_duration}")
                logger.info("👋 Thank you for using Universal Data Quality Framework!")
                
                print("\n" + "="*60)
                print("📊 SESSION SUMMARY:")
                print(f"   • Started: {session_start.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"   • Ended: {session_end.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"   • Duration: {session_duration}")
                print("="*60)
                print("\n👋 Thank you for using Universal Data Quality Framework!")
                print("⭐ Data quality matters! ⭐")
                break
            else:
                logger.warning(f"Invalid user input received: {choice}")
                print("❌ Invalid choice. Please enter 1-6.")
                
        except KeyboardInterrupt:
            logger.info("Module interrupted by user (Ctrl+C)")
            print("\n⚠️  Operation cancelled by user. Returning to main menu...")
            continue
        except Exception as e:
            logger.error(f"Error in module execution for choice {choice}: {str(e)}", exc_info=True)
            print(f"\n❌ An error occurred: {e}")
            print("Please try again or contact support.")
            
            # Show memory usage if error might be memory-related
            if "memory" in str(e).lower() or "Memory" in str(e):
                try:
                    process = psutil.Process(os.getpid())
                    memory_mb = process.memory_info().rss / 1024 / 1024
                    print(f"💾 Current memory usage: {memory_mb:.1f} MB")
                    print("💡 Try using sampling or a smaller dataset.")
                except:
                    pass

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Framework terminated by user (Ctrl+C)")
        print("\n\n⚠️  Framework terminated by user.")
    except Exception as e:
        logger.critical(f"Critical error in main execution: {str(e)}", exc_info=True)
        print(f"\n💥 Critical error: {e}")
        print("Please check the log file for details.")