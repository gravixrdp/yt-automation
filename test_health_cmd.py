import sys
import asyncio
from unittest.mock import AsyncMock, MagicMock

# Import bot modules
import telegram_bot
import queue_db

# Init DB for stats
queue_db.init_db()

# Mock admin check to always return True
telegram_bot.is_admin = lambda uid: True

async def run_test():
    print("Test started.")
    
    # Mock update object
    update = MagicMock()
    update.effective_user.id = 12345
    update.message = MagicMock()
    update.message.reply_text = AsyncMock()
    
    # Mock context
    context = MagicMock()
    
    try:
        # Call the handler
        await telegram_bot.cmd_health(update, context)
        
        # Check if reply_text was called
        if update.message.reply_text.called:
            args = update.message.reply_text.call_args
            msg = args[0][0]
            print("\n✅ /health command output:")
            print("-" * 40)
            print(msg)
            print("-" * 40)
            
            # Verify specific content
            if "System Health" in msg and "Queue DB" in msg:
                 print("✅ Content verification passed")
            else:
                 print("❌ Content verification failed: missing key phrases")
        else:
            print("❌ cmd_health did not call reply_text")
    except Exception as e:
        print(f"❌ Exception during test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(run_test())
