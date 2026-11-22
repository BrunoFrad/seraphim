pub mod cli;
pub mod db;
pub mod server;

pub fn banner(port: u16) {
    let banner_content: &str =
        &format!("\t󰈈 \tSeraphim is watching\n\t \tServer is listening on: ws://127.0.0.1:{port}");

    let banner_style = lipgloss::Style::new()
        .height(4)
        .width(56)
        .margin(2, 2, 2, 2)
        .align(lipgloss::position::LEFT, lipgloss::position::CENTER)
        .border(lipgloss::border::rounded_border());

    println!("{}", banner_style.render(banner_content));
}
