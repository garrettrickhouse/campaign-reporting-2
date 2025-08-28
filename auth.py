import streamlit as st
import os

def check_authentication():
    """Check if user is authenticated"""
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    return st.session_state.authenticated

def show_login_page():
    """Display the login page with password authentication"""
    # Note: page_config is set in main app, don't override here
    
    # Center the login form
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
        <div style='text-align: center; margin-bottom: 2rem;'>
            <h1>🔐 Access Required</h1>
            <p style='font-size: 1.1rem; color: #666;'>Please enter the password to access the reporting dashboard</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Create a styled container for the login form
        with st.container():
            st.markdown("""
            <div style='background-color: #f0f2f6; padding: 2rem; border-radius: 10px; border: 1px solid #e0e0e0;'>
            """, unsafe_allow_html=True)
            
            # Password input field
            password = st.text_input(
                "Password",
                type="password",
                placeholder="Enter your password",
                key="password_input"
            )
            
            # Login button
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                if st.button("🔑 Login", type="primary", use_container_width=True):
                    if check_password(password):
                        st.session_state.authenticated = True
                        st.success("✅ Authentication successful!")
                        st.rerun()
                    else:
                        st.error("❌ Incorrect password. Please try again.")
                        # Note: We can't clear password_input after widget instantiation
                        # The error message will remain visible until next interaction
            
            st.markdown("</div>", unsafe_allow_html=True)
        
        # Add some spacing
        st.markdown("<br><br>", unsafe_allow_html=True)
        
        # Footer
        st.markdown("""
        <div style='text-align: center; color: #888; font-size: 0.9rem;'>
            <p>Jones Road Beauty - Reporting Dashboard</p>
        </div>
        """, unsafe_allow_html=True)

def check_password(password):
    """Check if the provided password is correct"""
    # Get password from environment variable, fallback to a default
    correct_password = os.getenv('APP_PASSWORD', 'rickhouse')
    
    # Simple password comparison
    return password == correct_password

def show_logout_button():
    """Show logout button in sidebar for authenticated users"""
    with st.sidebar:
        if st.button("🚪 Logout", type="secondary"):
            st.session_state.authenticated = False
            st.rerun()

def require_authentication():
    """Decorator-style function to require authentication before showing content"""
    if not check_authentication():
        show_login_page()
        return False
    return True
